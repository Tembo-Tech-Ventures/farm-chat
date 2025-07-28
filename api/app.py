from flask import Flask, jsonify, abort
import zipfile
import pandas as pd
import uuid
import os
import download_data

DATA_ZIP_PATH = os.path.join(os.path.dirname(__file__), 'data', '34053d6d-580c-400f-9b40-ef9184f30567')
NAMESPACE = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')  # UUID namespace for v5

app = Flask(__name__)

def ensure_data_downloaded():
    """Ensure data is downloaded before accessing it."""
    if not os.path.exists(DATA_ZIP_PATH):
        download_data.download_data()

def get_csv_files():
    ensure_data_downloaded()
    with zipfile.ZipFile(DATA_ZIP_PATH, 'r') as zip_ref:
        return [f for f in zip_ref.namelist() if f.endswith('.csv')]

def get_file_id(filename):
    return str(uuid.uuid5(NAMESPACE, filename))

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'})

@app.route('/files', methods=['GET'])
def list_files():
    ensure_data_downloaded()
    files = get_csv_files()
    result = [
        {'name': f, 'id': get_file_id(f)}
        for f in files
    ]
    return jsonify(result)

@app.route('/files/<file_id>/metadata', methods=['GET'])
def file_metadata(file_id):
    ensure_data_downloaded()
    files = get_csv_files()
    file_map = {get_file_id(f): f for f in files}
    if file_id not in file_map:
        abort(404, description='File not found')
    filename = file_map[file_id]
    with zipfile.ZipFile(DATA_ZIP_PATH, 'r') as zip_ref:
        with zip_ref.open(filename) as csvfile:
            try:
                df = pd.read_csv(csvfile)
            except UnicodeDecodeError:
                csvfile.seek(0)
                df = pd.read_csv(csvfile, encoding='latin1')
    metadata = {
        'name': filename,
        'id': file_id,
        'row_count': int(df.shape[0]),
        'column_count': int(df.shape[1]),
        'columns': df.columns.tolist(),
        'sample_rows': df.sample(min(5, len(df)), random_state=42).to_dict(orient='records')
    }
    return jsonify(metadata)
