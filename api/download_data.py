import os
import requests

def download_data():
    url = "https://data.mendeley.com/public-files/datasets/dkv6b3xj99/files/34053d6d-580c-400f-9b40-ef9184f30567/file_downloaded"
    out_path = os.path.join(os.path.dirname(__file__), 'data', '34053d6d-580c-400f-9b40-ef9184f30567')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    if not os.path.exists(out_path):
        print(f"Downloading data to {out_path}...")
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(out_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print("Download complete.")
    else:
        print("Data file already exists.")

if __name__ == "__main__":
    download_data()
