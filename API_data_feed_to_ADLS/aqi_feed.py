from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Load .env reliably from the same folder
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

DATA_GOV_API_KEY = os.getenv("DATA_GOV_API_KEY")
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
RESOURCE_ID = "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
CONTAINER_NAME = "aqi-data"

# Lower limit to 100 so data.gov.in responds quickly without timing out
API_URL = (
    f"https://api.data.gov.in/resource/{RESOURCE_ID}"
    f"?api-key={DATA_GOV_API_KEY}&format=json&limit=100"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like"
        " Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def fetch_and_land_cpcb_data():
  if not DATA_GOV_API_KEY or not AZURE_CONN_STR:
    logging.error("Missing credentials in .env file.")
    return

  try:
    logging.info("Calling data.gov.in CPCB endpoint (limit=100)...")
    # Extended timeout to 60 seconds with browser headers
    response = requests.get(API_URL, headers=HEADERS, timeout=60)
    response.raise_for_status()

    payload = response.json()
    records = payload.get("records", [])
    logging.info(f"Retrieved {len(records)} pollutant records successfully.")

    if not records:
      logging.warning("API returned 0 records. Check response structure.")
      return

    # Build unique partitioned landing path
    now = datetime.now(timezone.utc)
    file_name = f"cpcb_telemetry_{now.strftime('%Y%m%d_%H%M%S')}.json"
    blob_path = (
        f"cpcb_raw/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{file_name}"
    )

    # Upload to ADLS Gen2
    logging.info(f"Connecting to ADLS Gen2 container: '{CONTAINER_NAME}'...")
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    blob_client = blob_service_client.get_blob_client(
        container=CONTAINER_NAME, blob=blob_path
    )

    json_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode(
        "utf-8"
    )
    blob_client.upload_blob(json_bytes, overwrite=True)

    logging.info(f"File uploaded to ADLS Gen2: {CONTAINER_NAME}/{blob_path}")

  except requests.exceptions.Timeout:
    logging.error(
        "Request timed out. The data.gov.in server is slow; try again in a"
        " moment."
    )
  except requests.exceptions.RequestException as e:
    logging.error(f"API request failed: {e}")
  except Exception as e:
    logging.error(f"Upload failed: {e}")


if __name__ == "__main__":
  fetch_and_land_cpcb_data()