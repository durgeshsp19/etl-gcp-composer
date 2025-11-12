# scripts/fetch_api_and_upload.py
import json
import requests
from google.cloud import storage
from datetime import datetime

API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"  # example

def fetch_and_upload_to_gcs(gcs_path):
    # gcs_path e.g. gs://bucket/raw/2025-11-12/data.json
    print(f"Fetching API: {API_URL}")
    r = requests.get(API_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    # add metadata
    payload = {
        "fetched_at": datetime.utcnow().isoformat(),
        "data": data
    }
    # upload
    storage_client = storage.Client()
    # parse bucket + object
    if not gcs_path.startswith("gs://"):
        raise ValueError("Expecting gs:// path")
    _, rest = gcs_path.split("gs://", 1)
    bucket_name, object_name = rest.split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    print(f"Uploaded raw JSON to {gcs_path}")
