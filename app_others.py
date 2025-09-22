import os
import csv
import time
import logging
import json
from io import StringIO, BytesIO
from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
from zenrows import ZenRowsClient
from bs4 import BeautifulSoup
import uuid
import threading
import re
from urllib.parse import urlencode

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


app = Flask(__name__)
app.secret_key = os.urandom(24)  # Needed for session management
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Initialize ZenRows client with retry configuration
ZENROWS_API_KEY = "1f29ccc2c252fde8de03420b538a1fce2d0de21a"
client = ZenRowsClient(
    ZENROWS_API_KEY,
    retries=3,          # Number of retries
    backoff=1,          # Backoff factor
    timeout=30          # Timeout in seconds
)

# Store scraping jobs (in production, use a database)
scraping_jobs = {}

def make_zenrows_request(url, params, max_retries=3):
    """Make a request with retry logic"""
    for attempt in range(max_retries):
        try:
            response = client.get(url, params=params)
            if response.status_code == 200:
                return response
            elif response.status_code in [403, 429, 500, 502, 503, 504]:
                logger.warning(f"Attempt {attempt+1} failed with status {response.status_code}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Request failed with status code: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Request failed with exception: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                return None
    return None

def extract_person_details(html_content, job_id, person_index, total_persons):
    """Extract person details from TruePeopleSearch detail page"""
    soup = BeautifulSoup(html_content, 'html.parser')

    # Send progress update via Socket.IO
    socketio.emit('progress_update', {
        'job_id': job_id,
        'message': f'Extracting details for person {person_index+1}/{total_persons}',
        'progress': (person_index / total_persons) * 100
    }, room=job_id)

    # Initialize with default values
    person_data = {
        'first_name': 'N/A',
        'last_name': 'N/A',
        'phone_numbers': [],
        'email_addresses': [],
        'mailing_address': 'N/A',
        'age': 'N/A',
        'associated_names': [],
        'relatives': [],
        'address_history': []
    }

    try:
        # Extract name from the main heading
        name_elem = soup.select_one('h1.oh1')
        if name_elem:
            name_parts = name_elem.text.strip().split()
            if name_parts:
                person_data['first_name'] = name_parts[0]
                if len(name_parts) > 1:
                    person_data['last_name'] = name_parts[-1]

        # Extract age
        age_elem = soup.select_one('.content-value')
        if age_elem:
            age_text = age_elem.text.strip()
            age_match = re.search(r'(\d+)', age_text)
            if age_match:
                person_data['age'] = age_match.group(1)

        # Extract current address
        address_elem = soup.select_one('.content-value.address')
        if not address_elem:
            address_elems = soup.select('.content-value')
            for elem in address_elems:
                if any(word in elem.text.lower() for word in ['ave', 'st', 'street', 'road', 'rd', 'ln']):
                    person_data['mailing_address'] = elem.text.strip()
                    break
        else:
            person_data['mailing_address'] = address_elem.text.strip()

        # Extract phone numbers
        phone_elems = soup.select('[data-link-to-more="phone"]')
        for phone_elem in phone_elems:
            phone_text = phone_elem.text.strip()
            if phone_text and phone_text not in person_data['phone_numbers']:
                person_data['phone_numbers'].append(phone_text)

        # Extract email addresses
        email_elems = soup.select('[href^="mailto:"]')
        for email_elem in email_elems:
            email = email_elem.get('href', '').replace('mailto:', '')
            if email and email not in person_data['email_addresses']:
                person_data['email_addresses'].append(email)

        # Extract associated names (AKA)
        aka_section = soup.find('div', string=re.compile('Also Seen As|AKA', re.IGNORECASE))
        if aka_section:
            aka_container = aka_section.find_next('div')
            if aka_container:
                person_data['associated_names'] = [span.text.strip() for span in aka_container.find_all('span')]

        # Extract relatives
        relative_section = soup.find('div', string=re.compile('Related to|Relatives', re.IGNORECASE))
        if relative_section:
            relative_container = relative_section.find_next('div')
            if relative_container:
                person_data['relatives'] = [span.text.strip() for span in relative_container.find_all('span')]

        # Extract address history
        address_section = soup.find('div', string=re.compile('Address History|Previous Addresses', re.IGNORECASE))
        if address_section:
            address_container = address_section.find_next('div')
            if address_container:
                person_data['address_history'] = [span.text.strip() for span in address_container.find_all('span')]

    except Exception as e:
        logger.error(f"Error extracting person details: {e}")

    return person_data



def search_address(address, city, state, zip_code, job_id, address_index, total_addresses):
    """Search for people at a specific address"""
    try:
        # Form the search query
        search_query = f"{address}, {city}, {state} {zip_code}"

        # Send progress update via Socket.IO
        socketio.emit('progress_update', {
            'job_id': job_id,
            'message': f'Searching address {address_index+1}/{total_addresses}: {search_query}',
            'progress': (address_index / total_addresses) * 50
        }, room=job_id)

        # Build the search URL - Using the correct endpoint from your example
        base_url = "https://www.truepeoplesearch.com/resultaddress"
        params = {
            "streetaddress": address,
            "citystatezip": f"{city}, {state} {zip_code}"
        }

        encoded_params = urlencode(params)
        search_url = f"{base_url}?{encoded_params}"

        # Use ZenRows to scrape the page
        zenrows_params = {
            "js_render": "true",
            "premium_proxy": "true",
            "antibot": "true",
            "wait_for": ".card-summary",
            "wait": "5000"  # Wait 5 seconds for page to load
        }

        response = make_zenrows_request(search_url, zenrows_params)

        if not response:
            socketio.emit('error', {
                'job_id': job_id,
                'message': f'Failed to retrieve search results for {search_query} after multiple attempts'
            }, room=job_id)
            return []

        # Parse the HTML response
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all person results
        result_cards = soup.select('.card-summary')

        # Send progress update via Socket.IO
        socketio.emit('progress_update', {
            'job_id': job_id,
            'message': f'Found {len(result_cards)} people at {search_query}',
            'progress': (address_index / total_addresses) * 50 + 25
        }, room=job_id)

        people_data = []
        for i, card in enumerate(result_cards):
            try:
                # Extract the detail page URL from the href attribute
                detail_link = card.get('data-detail-link')
                if not detail_link:
                    # Try to find it in the anchor tag
                    anchor = card.select_one('a.detail-link')
                    if anchor and anchor.get('href'):
                        detail_link = anchor.get('href')
                    else:
                        continue

                # Construct full URL
                detail_url = f"https://www.truepeoplesearch.com{detail_link}"

                # Scrape the detail page
                detail_response = make_zenrows_request(detail_url, zenrows_params)

                if not detail_response:
                    logger.error(f"Failed to retrieve detail page for {detail_url}")
                    continue

                # Extract detailed information
                person_details = extract_person_details(
                    detail_response.text,
                    job_id,
                    i,
                    len(result_cards)
                )

                # Add search query to person details
                person_details['search_address'] = search_query

                # Add to results
                people_data.append(person_details)

                # Send person data via Socket.IO
                socketio.emit('person_data', {
                    'job_id': job_id,
                    'person': person_details,
                    'total_persons': len(result_cards),
                    'current_index': i+1
                }, room=job_id)

                # Be respectful with requests
                time.sleep(2)

            except Exception as e:
                logger.error(f"Error processing person result: {e}")
                continue

        return people_data

    except Exception as e:
        logger.error(f"Error searching address: {e}")
        socketio.emit('error', {
            'job_id': job_id,
            'message': f'Error searching address: {str(e)}'
        }, room=job_id)
        return []

def process_address(row, job_id, address_index, total_addresses):
    """Process a single address and scrape data from TruePeopleSearch"""
    try:
        # Extract address components
        address = row.get('Address', '')
        city = row.get('City', '')
        state = row.get('State', '')
        zip_code = row.get('Zip', '')

        # Search for people at this address
        people_data = search_address(address, city, state, zip_code, job_id, address_index, total_addresses)

        # Update job status
        scraping_jobs[job_id]['processed'] += 1

        return people_data

    except Exception as e:
        logger.error(f"Error processing address: {e}")
        socketio.emit('error', {
            'job_id': job_id,
            'message': f'Error processing address: {str(e)}'
        }, room=job_id)
        return []

def process_csv_job(job_id, csv_data):
    """Process all addresses in the CSV"""
    try:
        # Read CSV data
        csv_file = StringIO(csv_data)
        reader = csv.DictReader(csv_file)
        rows = list(reader)

        # Initialize job data
        scraping_jobs[job_id] = {
            'total': len(rows),
            'processed': 0,
            'status': 'Starting...',
            'results': [],
            'completed': False
        }

        # Send initial progress via Socket.IO
        socketio.emit('job_started', {
            'job_id': job_id,
            'total_addresses': len(rows)
        }, room=job_id)

        # Process each row
        for i, row in enumerate(rows):
            people_data = process_address(row, job_id, i, len(rows))

            # Add to job results
            scraping_jobs[job_id]['results'].extend(people_data)

            # Be respectful with requests
            time.sleep(3)

        # Mark job as completed
        scraping_jobs[job_id]['status'] = 'Completed'
        scraping_jobs[job_id]['completed'] = True

        # Send completion event via Socket.IO
        socketio.emit('job_completed', {
            'job_id': job_id,
            'total_results': len(scraping_jobs[job_id]['results'])
        }, room=job_id)

    except Exception as e:
        logger.error(f"Error processing CSV job: {e}")
        scraping_jobs[job_id]['status'] = f'Error: {str(e)}'
        scraping_jobs[job_id]['completed'] = True

        # Send error event via Socket.IO
        socketio.emit('error', {
            'job_id': job_id,
            'message': f'Error processing CSV job: {str(e)}'
        }, room=job_id)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        if not file.filename.endswith('.csv'):
            return jsonify({'error': 'File must be a CSV'}), 400

        # Read CSV content
        csv_data = file.read().decode('utf-8')

        # Create a job ID
        job_id = str(uuid.uuid4())

        # Start processing in a separate thread
        thread = threading.Thread(
            target=process_csv_job,
            args=(job_id, csv_data)
        )
        thread.daemon = True
        thread.start()

        return jsonify({'job_id': job_id})

    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/status/<job_id>')
def get_status(job_id):
    if job_id not in scraping_jobs:
        return jsonify({'error': 'Job not found'}), 404

    job_data = scraping_jobs[job_id]
    return jsonify({
        'total': job_data['total'],
        'processed': job_data['processed'],
        'status': job_data['status'],
        'completed': job_data['completed'],
        'results_count': len(job_data['results'])
    })

@app.route('/results/<job_id>')
def get_results(job_id):
    if job_id not in scraping_jobs:
        return jsonify({'error': 'Job not found'}), 404

    return jsonify(scraping_jobs[job_id]['results'])

@app.route('/download/<job_id>')
def download_results(job_id):
    if job_id not in scraping_jobs:
        return "Job not found", 404

    # Create CSV in memory as bytes
    output = BytesIO()

    # Create a CSV writer that writes to the BytesIO object
    writer = csv.writer(output)

    # Write header
    writer.writerow([
        'First Name', 'Last Name', 'Age', 'Search Address',
        'Phone Numbers', 'Email Addresses',
        'Mailing Address', 'Associated Names',
        'Relatives', 'Address History'
    ])

    # Write data
    for result in scraping_jobs[job_id]['results']:
        writer.writerow([
            result['first_name'],
            result['last_name'],
            result['age'],
            result.get('search_address', ''),
            '; '.join(result['phone_numbers']),
            '; '.join(result['email_addresses']),
            result['mailing_address'],
            '; '.join(result['associated_names']),
            '; '.join(result['relatives']),
            '; '.join(result['address_history'])
        ])

    # Prepare response - seek to beginning and send as file
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name=f'skiptrace_results_{job_id}.csv',
        mimetype='text/csv'
    )

@socketio.on('connect')
def handle_connect():
    logger.info('Client connected')

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

@socketio.on('join_job')
def handle_join_job(data):
    job_id = data.get('job_id')
    if job_id:
        socketio.emit('message', {'data': f'Joined job room: {job_id}'}, room=job_id)

if __name__ == '__main__':
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True)
