import os
import csv
import requests
import time
import json
from io import StringIO
from flask import Flask, render_template, request, send_file, flash, redirect, url_for, jsonify
from flask_socketio import SocketIO, emit
import pandas as pd
from werkzeug.utils import secure_filename
import re

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this for production
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Initialize SocketIO
socketio = SocketIO(app, cors_allowed_origins="*")

# API Configuration
ATTOM_API_KEY = '92db4efe575b60e254108004b28bc522'
ATTOM_BASE_URL = 'https://api.gateway.attomdata.com/propertyapi/v1.0.0'

# Ensure upload directory exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Store processing status
processing_status = {}

def query_attom_property_detail(address, city, state, zip_code):
    """Query ATTOM API for property details"""
    headers = {
        'apikey': ATTOM_API_KEY,
        'Accept': 'application/json'
    }

    params = {
        'address1': address,
        'address2': f"{city}, {state} {zip_code}"
    }

    try:
        url = f"{ATTOM_BASE_URL}/property/detail"
        print(f"Querying ATTOM API: {url} with params: {params}")
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"ATTOM API Error for address '{address}, {city}': {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error querying ATTOM API for address '{address}, {city}': {e}")
        return None

def extract_owner_info_from_attom(attom_data):
    """Extract owner information from ATTOM API response"""
    if not attom_data or 'property' not in attom_data or not attom_data['property']:
        return {}

    property_data = attom_data['property'][0]
    owner_data = property_data.get('owner', {})

    # Extract owner names
    owners = []
    for i in range(1, 5):
        owner_key = f'owner{i}'
        if owner_data.get(owner_key, {}).get('firstName') or owner_data.get(owner_key, {}).get('lastName'):
            owners.append({
                'first_name': owner_data.get(owner_key, {}).get('firstName', ''),
                'last_name': owner_data.get(owner_key, {}).get('lastName', '')
            })

    # Extract mailing address
    mailing_address = owner_data.get('mailingAddress', {})
    mailing_addr_str = f"{mailing_address.get('line1', '')}"
    if mailing_address.get('line2'):
        mailing_addr_str += f", {mailing_address.get('line2')}"

    return {
        'owners': owners,
        'mailing_address': mailing_addr_str,
        'mailing_city': mailing_address.get('city', ''),
        'mailing_state': mailing_address.get('state', ''),
        'mailing_zip': mailing_address.get('zip', ''),
        'mailing_zip4': mailing_address.get('zip4', '')
    }

def get_contact_info_from_whitepages(owner_name, mailing_address, city, state, zip_code):
    """Get contact information using WhitePages API or similar service"""
    # TODO: Implement actual integration with a contact information API like WhitePages or Melissa.
    # This function currently returns empty data.
    #
    # Example API call structure (this is hypothetical):
    #
    # contact_api_key = "YOUR_CONTACT_API_KEY"
    # contact_api_url = "https://api.contactprovider.com/lookup"
    # headers = {'apikey': contact_api_key}
    # params = {
    #     'name': owner_name,
    #     'address': mailing_address,
    #     'city': city,
    #     'state': state,
    #     'zip': zip_code
    # }
    # response = requests.get(contact_api_url, headers=headers, params=params)
    # if response.status_code == 200:
    #     data = response.json()
    #     return {
    #         'phone_numbers': data.get('phone', ''),
    #         'email_address': data.get('email', '')
    #     }

    return {
        'phone_numbers': '',
        'email_address': ''
    }

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('connect')
def handle_connect():
    print(f'Client connected with sid: {request.sid}')
    emit('session_id', {'sid': request.sid})

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    session_id = request.form.get('sid')
    if not session_id:
        return jsonify({'error': 'Missing session ID'}), 400

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        processing_status[session_id] = {
            'filename': filename,
            'progress': 0,
            'total': 0,
            'current': 0
        }

        # Process the CSV file in background
        socketio.start_background_task(target=process_csv, filepath=filepath, session_id=session_id)

        return jsonify({'status': 'processing', 'session_id': session_id})

    return jsonify({'error': 'Invalid file type'}), 400

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'csv'

def process_csv(filepath, session_id):
    """Process the uploaded CSV and enhance with owner information"""
    try:
        df = pd.read_csv(filepath)
        total_rows = len(df)

        # Update status
        processing_status[session_id]['total'] = total_rows
        processing_status[session_id]['current'] = 0
        socketio.emit('progress', {
            'current': 0,
            'total': total_rows,
            'percent': 0
        }, room=session_id)

        # Create a new DataFrame for the results
        result_df = pd.DataFrame(columns=[
            'Property_Address',
            'Owner_First_Name',
            'Owner_Last_Name',
            'Phone_Numbers',
            'Mailing_Address',
            'Email_Address',
            'Source'
        ])

        for index, row in df.iterrows():
            # Update progress
            processing_status[session_id]['current'] = index + 1
            progress_percent = int(((index + 1) / total_rows) * 100)

            socketio.emit('progress', {
                'current': index + 1,
                'total': total_rows,
                'percent': progress_percent,
                'address': f"{row.get('Address', '')}, {row.get('City', '')}"
            }, room=session_id)

            address = row.get('Address', '')
            city = row.get('City', '')
            state = row.get('State', '')
            zip_code = str(row.get('Zip', '')).split('.')[0]  # Remove decimal if present

            # Skip if essential address components are missing
            if not address or not city or not state:
                continue

            # Try ATTOM API first for property details
            attom_data = query_attom_property_detail(address, city, state, zip_code)
            owner_info = extract_owner_info_from_attom(attom_data) if attom_data else {}

            if owner_info and owner_info.get('owners'):
                # Use the first owner found
                first_owner = owner_info['owners'][0]
                first_name = first_owner.get('first_name', '')
                last_name = first_owner.get('last_name', '')

                # Format mailing address
                mailing_parts = [
                    owner_info.get('mailing_address', ''),
                    owner_info.get('mailing_city', ''),
                    owner_info.get('mailing_state', ''),
                    owner_info.get('mailing_zip', '')
                ]
                mailing_parts = [part for part in mailing_parts if part]
                mailing_address = ', '.join(mailing_parts)

                # Try to get phone and email from contact API
                owner_name = f"{first_name} {last_name}"
                contact_info = get_contact_info_from_whitepages(
                    owner_name,
                    owner_info.get('mailing_address', ''),
                    owner_info.get('mailing_city', ''),
                    owner_info.get('mailing_state', ''),
                    owner_info.get('mailing_zip', '')
                )

                # Add to results
                result_df.loc[len(result_df)] = [
                    f"{address}, {city}, {state} {zip_code}",
                    first_name,
                    last_name,
                    contact_info.get('phone_numbers', ''),
                    mailing_address,
                    contact_info.get('email_address', ''),
                    'ATTOM API + Contact Enhancement'
                ]
            else:
                # If no owner info found, still add the property address
                result_df.loc[len(result_df)] = [
                    f"{address}, {city}, {state} {zip_code}",
                    '', '', '', '', '',
                    'No owner info found'
                ]

            # Add delay to respect API rate limits
            time.sleep(0.5)

        # Save the enhanced CSV
        output_filename = f"skiptrace_results_{os.path.basename(filepath)}"
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
        result_df.to_csv(output_path, index=False)

        # Update status
        processing_status[session_id]['output_filename'] = output_filename
        socketio.emit('complete', {
            'filename': output_filename,
            'message': 'Processing complete'
        }, room=session_id)

    except Exception as e:
        socketio.emit('error', {
            'message': f'Error processing file: {str(e)}'
        }, room=session_id)

@app.route('/download/<filename>')
def download_file(filename):
    return send_file(
        os.path.join(app.config['UPLOAD_FOLDER'], filename),
        as_attachment=True,
        download_name=filename
    )

@app.route('/api_info')
def api_info():
    """Display information about API integration"""
    api_info = {
        'ATTOM': {
            'coverage': 'Property data nationwide',
            'data_provided': 'Owner names, mailing addresses',
            'limitations': 'No phone numbers or email addresses',
            'cost': 'Varies by plan ($0.10 - $0.50 per property)',
            'link': 'https://api.developer.attomdata.com/plans'
        },
        'WhitePages': {
            'coverage': 'US contact information',
            'data_provided': 'Phone numbers, email addresses',
            'limitations': 'Separate service required',
            'cost': 'Varies by volume ($0.10 - $0.30 per lookup)',
            'link': 'https://www.whitepages.com/'
        },
        'Melissa': {
            'coverage': 'Global contact information',
            'data_provided': 'Phone numbers, email addresses',
            'limitations': 'Separate service required',
            'cost': 'Varies by product ($0.01 - $0.25 per lookup)',
            'link': 'https://www.melissa.com/pricing'
        }
    }
    return render_template('api_info.html', api_info=api_info)

@app.route('/sample_csv')
def sample_csv():
    """Provide a sample CSV template for download"""
    sample_data = [
        ['Id', 'Address', 'City', 'State', 'Zip', 'County'],
        ['1', '123 Main St', 'Columbus', 'OH', '43215', 'Franklin'],
        ['2', '456 Oak Ave', 'Cleveland', 'OH', '44101', 'Cuyahoga']
    ]

    si = StringIO()
    writer = csv.writer(si)
    writer.writerows(sample_data)

    output = send_file(
        StringIO(si.getvalue()),
        as_attachment=True,
        download_name="sample_properties.csv",
        mimetype='text/csv'
    )

    return output

if __name__ == '__main__':
    socketio.run(app, debug=True, port=5001)
