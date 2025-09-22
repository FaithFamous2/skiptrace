# app.py
import os
import re
import csv
import time
import requests
from flask import Flask, render_template, request, send_file, jsonify, Response
from bs4 import BeautifulSoup
import urllib.parse
import io
import logging
from datetime import datetime
import json
from typing import List, Dict, Any, Optional, Tuple
import threading
import random
from dotenv import load_dotenv
from collections import deque

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Configuration (make wait configurable)
ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")
ZENROWS_BASE_URL = "https://api.zenrows.com/v1/"
TRUE_PEOPLE_SEARCH_BASE = "https://www.truepeoplesearch.com"
ZENROWS_WAIT_MS = int(os.getenv("ZENROWS_WAIT_MS", "5000"))  # default 5000 (5s)
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "15.0"))

# Global dictionary to track the state of multiple processing jobs (in-memory)
processing_jobs: Dict[str, Dict[str, Any]] = {}
RESULTS_FOLDER = 'results'
os.makedirs(RESULTS_FOLDER, exist_ok=True)


class SkipTracer:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.request_count = 0
        self.last_request_time = time.time()

    def expand_address(self, address: str) -> List[str]:
        addresses = []

        if '#' in address:
            parts = address.split('#')
            base_address = parts[0].strip()
            number_part = parts[1].strip()
            base_match = re.search(r'(\d+)', base_address)
            if base_match and number_part.isdigit():
                base_num = int(base_match.group(1))
                end_num = int(number_part)
                if end_num > base_num:
                    for num in range(base_num, end_num + 1):
                        expanded = re.sub(r'\d+', str(num), base_address, count=1)
                        addresses.append(expanded)
                    return addresses

        hyphen_match = re.search(r'(\d+)-(\d+)\s+', address)
        if hyphen_match:
            start = int(hyphen_match.group(1))
            end = int(hyphen_match.group(2))
            if end < start and len(str(end)) < len(str(start)):
                prefix = str(start)[:len(str(start)) - len(str(end))]
                end = int(prefix + str(end))
            if end >= start:
                for num in range(start, end + 1):
                    expanded = re.sub(r'\d+-\d+', str(num), address, count=1)
                    addresses.append(expanded)
                return addresses

        addresses.append(address)
        return addresses

    def make_zenrows_request(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """
        Make a request to Zenrows with retry logic and proper parameters
        """
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        # Ensure at least 2 seconds between requests to avoid rate limiting
        if time_since_last_request < 2:
            time.sleep(2 - time_since_last_request)

        self.last_request_time = time.time()

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = 2 ** attempt + random.uniform(0, 1)
                    logger.info(f"Waiting {wait_time:.2f} seconds before retry...")
                    time.sleep(wait_time)

                params = {
                    'url': url,
                    'apikey': self.api_key,
                    'js_render': 'true',
                    'premium_proxy': 'true',
                    'proxy_country': 'us',
                    'wait': str(ZENROWS_WAIT_MS),  # configurable
                    'block_resources': 'image,media,font',
                }

                response = self.session.get(
                    ZENROWS_BASE_URL,
                    params=params,
                    timeout=30
                )

                if response.status_code == 200:
                    self.request_count += 1
                    return response
                elif response.status_code == 429:
                    logger.error(f"Attempt {attempt + 1}: Rate limited (429). Waiting longer...")
                    time.sleep(10 * (attempt + 1))
                    continue
                else:
                    logger.error(f"Attempt {attempt + 1}: Failed to fetch data: {response.status_code}")
                    if response.text:
                        logger.debug(f"Response content (truncated): {response.text[:500]}")

            except requests.exceptions.Timeout:
                logger.error(f"Attempt {attempt + 1}: Timeout error")
            except requests.exceptions.RequestException as e:
                logger.error(f"Attempt {attempt + 1}: Request error: {str(e)}")

        return None

    def search_address(self, address: str, city: str, state: str, zip_code: str) -> Optional[BeautifulSoup]:
        search_url = f"{TRUE_PEOPLE_SEARCH_BASE}/results"
        query_params = {
            'streetaddress': address,
            'citystatezip': f"{city}, {state} {zip_code}"
        }

        job_id = threading.current_thread().name
        if job_id in processing_jobs:
            processing_jobs[job_id]['results'].append({
                'type': 'status',
                'message': f"Searching for: {address}, {city}, {state} {zip_code}",
                'url': f"{search_url}?{urllib.parse.urlencode(query_params)}"
            })

        target_url = f"{search_url}?{urllib.parse.urlencode(query_params)}"
        logger.info(f"Requesting URL: {target_url}")

        response = self.make_zenrows_request(target_url)
        if response:
            if "Please enable JavaScript to view the page content" in response.text:
                logger.error("JavaScript rendering failed - page requires JavaScript")
                if job_id in processing_jobs:
                    processing_jobs[job_id]['results'].append({
                        'type': 'status',
                        'message': "JavaScript rendering failed - try increasing wait time"
                    })
                return None

            soup = BeautifulSoup(response.content, 'html.parser')

            record_count_elem = soup.find('div', class_='h2')
            if job_id in processing_jobs:
                if record_count_elem and "No Results Found" in record_count_elem.text:
                    processing_jobs[job_id]['results'].append({
                        'type': 'status',
                        'message': "No results found for this address"
                    })
                    logger.info("No results found for this address")
                    return soup
                elif record_count_elem:
                    result_text = record_count_elem.text.strip()
                    processing_jobs[job_id]['results'].append({
                        'type': 'status',
                        'message': f"Found results: {result_text}"
                    })
                    logger.info(f"Found: {result_text}")

                    match = re.search(r'(\d+)', result_text)
                    if match:
                        processing_jobs[job_id]['stats']['people_found'] += int(match.group(1))

            return soup
        else:
            if job_id in processing_jobs:
                processing_jobs[job_id]['results'].append({
                    'type': 'status',
                    'message': f"Failed to search for: {address}, {city}, {state} {zip_code}"
                })
        return None

    def parse_search_results(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        results = []

        no_results = soup.find('div', class_='h2', string=lambda text: text and 'No Results Found' in text)
        if no_results:
            logger.info("No results found on this page")
            return results

        person_cards = soup.find_all('div', class_='card-summary')

        for card in person_cards:
            try:
                name_elem = card.find('div', class_='h4')
                if not name_elem:
                    name_elem = card.find('h2')

                if name_elem:
                    name_parts = name_elem.text.strip().split()
                    first_name = name_parts[0] if name_parts else ""
                    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
                else:
                    first_name, last_name = "", ""

                detail_link = card.find('a')
                detail_url = detail_link['href'] if detail_link and detail_link.has_attr('href') else ""

                age_location = card.find_all('span', class_='content-value')
                age = age_location[0].text.strip() if len(age_location) > 0 else ""
                location = age_location[1].text.strip() if len(age_location) > 1 else ""

                results.append({
                    'first_name': first_name,
                    'last_name': last_name,
                    'age': age,
                    'location': location,
                    'detail_url': detail_url
                })
            except Exception as e:
                logger.error(f"Error parsing person card: {str(e)}")
                continue

        return results

    def get_person_details(self, detail_url: str) -> Dict[str, Any]:
        details = {
            'first_name': '',
            'last_name': '',
            'phones': [],
            'emails': [],
            'addresses': [],
            'url': '',
            'error': None
        }

        try:
            full_url = f"{TRUE_PEOPLE_SEARCH_BASE}{detail_url}"
            details['url'] = full_url
            logger.info(f"Getting details for: {full_url}")

            job_id = threading.current_thread().name
            if job_id in processing_jobs:
                processing_jobs[job_id]['results'].append({
                    'type': 'status',
                    'message': f"Fetching details for: {full_url}",
                    'url': full_url
                })

            response = self.make_zenrows_request(full_url)

            if not response or response.status_code != 200:
                error_msg = f"HTTP Error: {response.status_code}" if response else "Failed to fetch details"
                details['error'] = error_msg
                if job_id in processing_jobs:
                    processing_jobs[job_id]['results'].append({'type': 'status', 'message': error_msg})
                return details

            soup = BeautifulSoup(response.content, 'html.parser')

            person_card = soup.find('div', id='personDetails')
            if not person_card:
                details['error'] = "Could not find personDetails card."
                if job_id in processing_jobs:
                    processing_jobs[job_id]['results'].append({'type': 'status', 'message': details['error']})
                return details

            details['first_name'] = person_card.get('data-fn', '')
            details['last_name'] = person_card.get('data-ln', '')

            if not details['first_name'] and not details['last_name']:
                name_elem = soup.find('h1', class_='oh1')
                if name_elem:
                    name_parts = name_elem.text.strip().split()
                    details['first_name'] = name_parts[0] if name_parts else ""
                    details['last_name'] = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

            phone_section = soup.find('h5', string='Phone Numbers')
            if phone_section:
                parent_div = phone_section.find_parent('div', class_='row')
                if parent_div:
                    phone_spans = parent_div.find_all('span', itemprop='telephone')
                    for span in phone_spans:
                        phone = span.text.strip()
                        if phone and phone not in details['phones']:
                            details['phones'].append(phone)

            bio_phones = person_card.select('a[href*="/find/phone/"]')
            for phone_link in bio_phones:
                phone_text = phone_link.text.strip()
                if phone_text and re.match(r'\(\d{3}\) \d{3}-\d{4}', phone_text) and phone_text not in details['phones']:
                    details['phones'].append(phone_text)

            email_section = soup.find('h5', string='Email Addresses')
            if email_section:
                parent_div = email_section.find_parent('div', class_='row')
                if parent_div:
                    email_div = parent_div.find('div', class_=lambda c: c and 'col' in c and '@' in parent_div.text)
                    if email_div:
                        email_text = email_div.text.strip()
                        found_emails = re.findall(r'[\w\.-]+@[\w\.-]+', email_text)
                        for email in found_emails:
                            if email not in details['emails']:
                                details['emails'].append(email)

            bio_email_span = person_card.find('span', class_='bio-hl', string=re.compile(r'\S+@\S+'))
            if bio_email_span:
                email = bio_email_span.text.strip()
                if email not in details['emails']:
                    details['emails'].append(email)

            address_section = soup.find('h5', string='Current Address')
            if address_section:
                parent_div = address_section.find_parent('div', class_='row')
                if parent_div:
                    street = parent_div.find('span', itemprop='streetAddress')
                    locality = parent_div.find('span', itemprop='addressLocality')
                    region = parent_div.find('span', itemprop='addressRegion')
                    postal_code = parent_div.find('span', itemprop='postalCode')

                    if all([street, locality, region, postal_code]):
                        full_address = f"{street.text.strip()}, {locality.text.strip()}, {region.text.strip()} {postal_code.text.strip()}"
                        if full_address not in details['addresses']:
                            details['addresses'].append(full_address)

            if not details['addresses']:
                address_link = address_section.find_next('a', href=re.compile(r'/find/address/')) if address_section else None
                if address_link:
                    address_parts = [part.strip() for part in address_link.stripped_strings]
                    full_address = ' '.join(address_parts)
                    if full_address and full_address not in details['addresses']:
                        details['addresses'].append(full_address)

            bio_address_links = person_card.select('a[data-link-to-more="bio-address"]')
            processed_hrefs = set()
            for link in bio_address_links:
                href = link.get('href')
                if href in processed_hrefs:
                    continue

                all_links_for_href = person_card.select(f'a[href="{href}"]')
                full_address_parts = []
                for part_link in all_links_for_href:
                    full_address_parts.append(' '.join(part_link.stripped_strings))

                processed_hrefs.add(href)
                full_address = ' '.join(full_address_parts)
                if full_address and full_address not in details['addresses']:
                    details['addresses'].append(full_address)

            # Update stats & push details event
            if job_id in processing_jobs:
                processing_jobs[job_id]['stats']['phones_found'] += len(details['phones'])
                processing_jobs[job_id]['stats']['emails_found'] += len(details['emails'])
                processing_jobs[job_id]['stats']['addresses_found'] += len(details['addresses'])

                processing_jobs[job_id]['results'].append({
                    'type': 'details',
                    'data': {
                        'first_name': details['first_name'],
                        'last_name': details['last_name'],
                        'phones': details['phones'],
                        'emails': details['emails'],
                        'addresses': details['addresses'],
                        'url': details['url']
                    }
                })

        except Exception as e:
            logger.exception(f"Error fetching person details: {str(e)}")
            details['error'] = str(e)

        return details

    def process_address(self, address: str, city: str, state: str, zip_code: str, county: str = "") -> List[Dict[str, Any]]:
        all_results = []
        expanded_addresses = self.expand_address(address)

        logger.info(f"Original address: {address}, Expanded to: {expanded_addresses}")

        job_id = threading.current_thread().name
        if job_id in processing_jobs:
            processing_jobs[job_id]['results'].append({
                'type': 'status',
                'message': f"Original address: {address}, Expanded to: {expanded_addresses}"
            })

        for exp_address in expanded_addresses:
            if job_id in processing_jobs and processing_jobs[job_id]['cancelled']:
                break

            if job_id in processing_jobs:
                processing_jobs[job_id]['stats']['addresses_processed'] += 1

            soup = self.search_address(exp_address, city, state, zip_code)
            if not soup:
                continue

            person_summaries = self.parse_search_results(soup)

            for person in person_summaries:
                job_id = threading.current_thread().name
                if job_id in processing_jobs and processing_jobs[job_id]['cancelled']:
                    break

                if job_id in processing_jobs:
                    processing_jobs[job_id]['results'].append({
                        'type': 'status',
                        'message': f"Getting details for: {person.get('first_name', '')} {person.get('last_name', '')}"
                    })

                details = self.get_person_details(person['detail_url'])
                person.update(details)
                all_results.append(person)

                time.sleep(2)  # delay between detail fetches

            time.sleep(3)  # respectful delay between address searches

        return all_results


# Initialize the skip tracer (API key may be None in dev)
skip_tracer = SkipTracer(ZENROWS_API_KEY)


def test_zenrows_connection():
    test_url = "https://www.truepeoplesearch.com"
    params = {
        'url': test_url,
        'apikey': ZENROWS_API_KEY,
        'js_render': 'true',
        'premium_proxy': 'true',
        'proxy_country': 'us',
        'wait': str(ZENROWS_WAIT_MS),
    }
    try:
        response = requests.get(ZENROWS_BASE_URL, params=params, timeout=30)
        if response.status_code == 200:
            logger.info("Zenrows connection test successful")
            logger.info(f"Response (truncated): {response.text[:120]}...")
            return True
        else:
            logger.error(f"Zenrows connection test failed: {response.status_code}")
            logger.debug(f"Response content: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Zenrows connection test error: {str(e)}")
        return False


# run a quick test at startup (safe to ignore failure)
test_zenrows_connection()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    if not file.filename.lower().endswith('.csv'):
        return jsonify({'error': 'File must be a CSV'}), 400

    job_id = f"job_{int(time.time())}_{random.randint(1000, 9999)}"
    output_filename = f"{job_id}_results.csv"
    output_filepath = os.path.join(RESULTS_FOLDER, output_filename)

    # Read file bytes immediately within the request context
    try:
        file_bytes = file.read()
    except Exception as e:
        logger.exception("Failed to read uploaded file")
        return jsonify({'error': 'Failed to read uploaded file'}), 400

    if not file_bytes:
        return jsonify({'error': 'Empty file uploaded'}), 400

    # Initialize job state with a bounded deque for events
    processing_jobs[job_id] = {
        'active': True,
        'cancelled': False,
        'results': deque(maxlen=500),  # keep last 500 events
        'current_row': 0,
        'total_rows': 0,
        'stats': {
            'addresses_processed': 0,
            'people_found': 0,
            'phones_found': 0,
            'emails_found': 0,
            'addresses_found': 0
        },
        'output_filepath': output_filepath,
        'output_filename': output_filename
    }

    def process_file(job_id_local: str, file_bytes_local: bytes):
        try:
            # Decode safely and parse CSV
            stream = io.StringIO(file_bytes_local.decode('utf-8', errors='replace'), newline=None)
            csv_input = csv.DictReader(stream)
            rows = list(csv_input)
            job = processing_jobs.get(job_id_local)
            if not job:
                logger.error("Job missing during processing")
                return

            job['total_rows'] = len(rows)

            # If no rows, finish early and write an empty output file header if possible
            if job['total_rows'] == 0:
                job['results'].append({
                    'type': 'complete',
                    'message': 'No rows to process',
                    'stats': job['stats']
                })
                job['active'] = False
                # create empty file with no content
                with open(job['output_filepath'], 'w', newline='', encoding='utf-8') as f:
                    pass
                return

            fieldnames = list(rows[0].keys()) + [
                "Owner's First Name", "Owner's Last Name", 'Phone Number(s)',
                'Mailing Address', 'Email Address', 'URL', 'full address'
            ]
            # write header once
            with open(job['output_filepath'], 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

            for i, row in enumerate(rows):
                if job['cancelled']:
                    break

                job['current_row'] = i + 1

                address = row.get('Address', '') or row.get('address', '')
                city = row.get('City', '') or row.get('city', '')
                state = row.get('State', '') or row.get('state', '')
                zip_code = row.get('Zip', '') or row.get('zip', '')
                county = row.get('County', '') or row.get('county', '')

                if address and city and state:
                    logger.info(f"Processing {i+1}/{job['total_rows']}: {address}, {city}, {state} {zip_code}")

                    progress_data = {
                        'type': 'progress',
                        'current': i + 1,
                        'total': job['total_rows'],
                        'address': address,
                        'stats': job['stats']
                    }
                    job['results'].append(progress_data)

                    people_data = skip_tracer.process_address(address, city, state, zip_code, county)

                    with open(job['output_filepath'], 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        if people_data:
                            for person in people_data:
                                result_row = row.copy()
                                result_row["Owner's First Name"] = person.get('first_name', '')
                                result_row["Owner's Last Name"] = person.get('last_name', '')
                                result_row['Phone Number(s)'] = '; '.join(person.get('phones', []))
                                result_row['Mailing Address'] = '; '.join(person.get('addresses', []))
                                result_row['Email Address'] = '; '.join(person.get('emails', []))
                                result_row['URL'] = person.get('url', '')
                                result_row['full address'] = address
                                writer.writerow(result_row)
                        else:
                            result_row = row.copy()
                            result_row["Owner's First Name"] = ''
                            result_row["Owner's Last Name"] = ''
                            result_row['Phone Number(s)'] = ''
                            result_row['Mailing Address'] = ''
                            result_row['Email Address'] = ''
                            result_row['URL'] = ''
                            result_row['full address'] = address
                            writer.writerow(result_row)
                else:
                    # write an empty result row if missing address fields
                    with open(job['output_filepath'], 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        result_row = row.copy()
                        result_row["Owner's First Name"] = ''
                        result_row["Owner's Last Name"] = ''
                        result_row['Phone Number(s)'] = ''
                        result_row['Mailing Address'] = ''
                        result_row['Email Address'] = ''
                        result_row['URL'] = ''
                        result_row['full address'] = ''
                        writer.writerow(result_row)

            # final event
            if not job['cancelled']:
                job['results'].append({
                    'type': 'complete',
                    'message': 'Processing completed successfully',
                    'stats': job['stats']
                })
            else:
                job['results'].append({
                    'type': 'cancelled',
                    'message': 'Processing cancelled by user',
                    'stats': job['stats']
                })

        except Exception as e:
            logger.exception(f"Error processing file for job {job_id_local}: {str(e)}")
            if job_id_local in processing_jobs:
                processing_jobs[job_id_local]['results'].append({
                    'type': 'error',
                    'message': f'Error processing file: {str(e)}'
                })
        finally:
            if job_id_local in processing_jobs:
                processing_jobs[job_id_local]['active'] = False

    # Start processing thread and return immediately
    thread = threading.Thread(target=process_file, args=(job_id, file_bytes), name=job_id)
    thread.daemon = True
    thread.start()

    return jsonify({'message': 'Processing started', 'job_id': job_id}), 202


@app.route('/stream/<job_id>')
def stream(job_id):
    def generate():
        last_index = 0
        last_sent = time.time()

        if job_id not in processing_jobs:
            # gracefully end if job doesn't exist
            yield f"data: {json.dumps({'type':'error','message':'Invalid job_id'})}\n\n"
            return

        while True:
            if job_id not in processing_jobs:
                break

            job = processing_jobs[job_id]

            # snapshot the deque so iteration is consistent
            results_snapshot = list(job['results'])

            while last_index < len(results_snapshot):
                result = results_snapshot[last_index]
                last_index += 1
                last_sent = time.time()
                try:
                    yield f"data: {json.dumps(result)}\n\n"
                except GeneratorExit:
                    return

            # break if done
            if not job['active'] and last_index >= len(results_snapshot):
                break

            # heartbeat to keep worker alive for Gunicorn timeouts
            if time.time() - last_sent > HEARTBEAT_INTERVAL:
                heartbeat = {'type': 'heartbeat', 'ts': int(time.time())}
                yield f"data: {json.dumps(heartbeat)}\n\n"
                last_sent = time.time()

            time.sleep(0.5)

    return Response(generate(), mimetype='text/event-stream')


@app.route('/cancel', methods=['POST'])
def cancel_processing():
    payload = request.get_json(silent=True) or {}
    job_id = payload.get('job_id')
    if job_id and job_id in processing_jobs:
        processing_jobs[job_id]['cancelled'] = True
        return jsonify({'message': 'Cancellation requested'})
    return jsonify({'error': 'Invalid job ID'}), 400


@app.route('/download/<job_id>')
def download_file(job_id):
    filename = f"{job_id}_results.csv"
    filepath = os.path.join(RESULTS_FOLDER, filename)

    if os.path.exists(filepath):
        return send_file(
            filepath,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
    return jsonify({'error': 'File not found or job ID is invalid'}), 404


@app.route('/status/<job_id>')
def get_status(job_id):
    if job_id in processing_jobs:
        job = processing_jobs[job_id]
        return jsonify({
            'active': job['active'],
            'cancelled': job['cancelled'],
            'current_row': job['current_row'],
            'total_rows': job['total_rows'],
            'stats': job['stats']
        })
    return jsonify({'error': 'Invalid job ID'}), 404


@app.template_filter('datetimeformat')
def datetimeformat(value, format='%Y-%m-%d %H:%M:%S'):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value).strftime(format)
    return value


@app.route('/downloads')
def list_downloads():
    files = []
    for filename in os.listdir(RESULTS_FOLDER):
        if filename.endswith('.csv'):
            job_id = filename.replace('_results.csv', '')
            filepath = os.path.join(RESULTS_FOLDER, filename)
            files.append({
                'name': filename,
                'job_id': job_id,
                'creation_time': os.path.getctime(filepath)
            })
    files.sort(key=lambda x: x['creation_time'], reverse=True)
    return render_template('download_jobs.html', files=files)


@app.route('/preview/<job_id>')
def preview_job(job_id):
    filename = f"{job_id}_results.csv"
    filepath = os.path.join(RESULTS_FOLDER, filename)

    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader, [])
            rows = list(reader)

        return jsonify({
            'headers': headers,
            'rows': rows
        })
    except Exception as e:
        logger.exception("Error previewing job file")
        return jsonify({'error': str(e)}), 500


@app.route('/view/<job_id>')
def view_job(job_id):
    filename = f"{job_id}_results.csv"
    filepath = os.path.join(RESULTS_FOLDER, filename)

    if not os.path.exists(filepath):
        return "File not found", 404

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        rows = list(reader)

    return render_template('view_job.html', job_id=job_id, filename=filename, headers=headers, rows=rows)


# duplicate /download route removed — use download_file above
if __name__ == '__main__':
    # For local testing - in production use gunicorn
    app.run(debug=True, threaded=True, port=int(os.getenv("PORT", 5001)))
