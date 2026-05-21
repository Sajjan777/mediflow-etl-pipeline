import requests
import pandas as pd
import logging
import time

API_ENDPOINT = "https://data.cms.gov/data-api/v1/dataset/9767cb68-8ea9-4f0b-8179-9431abc89f11/data"
BATCH_SIZE = 20000

def download_data(output_filepath):
    logging.info("Starting CMS Medicare Part D data download...")

    all_records = []
    offset = 0

    while True:
        params = {
            "size": BATCH_SIZE,
            "offset": offset,
        }

        logging.info(f"Fetching batch: offset={offset}, size={BATCH_SIZE}")

        try:
            response = requests.get(API_ENDPOINT, params=params, timeout=120)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error on offset {offset}: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Connection error on offset {offset}: {e}")
            raise
        except requests.exceptions.Timeout:
            logging.error(f"Request timed out at offset {offset}.")
            raise

        batch = response.json()

        if not batch:
            logging.info(f"No more records returned at offset {offset}. Download complete.")
            break

        all_records.extend(batch)
        logging.info(f"Total rows downloaded so far: {len(all_records)}")
        offset += BATCH_SIZE
        time.sleep(0.5)

    df = pd.DataFrame(all_records)
    df.to_csv(output_filepath, index=False)
    logging.info(f"Download complete. {len(df)} rows saved to {output_filepath}")
    return True