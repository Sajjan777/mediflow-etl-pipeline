import os
import logging
import time

import requests
import pandas as pd

# CMS Medicare Part D Prescribers dataset
# https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers
API_ENDPOINT = "https://data.cms.gov/data-api/v1/dataset/9767cb68-8ea9-4f0b-8179-9431abc89f11/data"
BATCH_SIZE = 10000  # CMS API hard cap is 5,000 rows per request

MAX_RETRIES = 3
RETRY_BACKOFF = 2.0  # seconds; doubles each retry
RATE_LIMIT_SLEEP = 0.5  # seconds between successful batches

logger = logging.getLogger(__name__)


def _fetch_batch_with_retry(offset: int) -> list:
    """Fetch a single batch from the API with retry on transient errors."""
    params = {"size": BATCH_SIZE, "offset": offset}
    last_exc = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Fetching batch: offset={offset}, size={BATCH_SIZE} (attempt {attempt}/{MAX_RETRIES})")
            response = requests.get(API_ENDPOINT, params=params, timeout=120)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                ValueError) as e:  # ValueError covers JSONDecodeError
            last_exc = e
            logger.warning(f"Attempt {attempt} failed at offset {offset}: {e}")
            if attempt < MAX_RETRIES:
                sleep_time = RETRY_BACKOFF * (2 ** (attempt - 1))
                logger.info(f"Retrying in {sleep_time}s...")
                time.sleep(sleep_time)

    logger.error(f"All {MAX_RETRIES} attempts failed at offset {offset}.")
    raise last_exc


def download_data(output_filepath: str) -> bool:
    """
    Download CMS Medicare Part D data in batches and save as CSV.

    Args:
        output_filepath: Destination path for the output CSV file.

    Returns:
        True on success.

    Raises:
        ValueError: If no records are returned from the API.
        requests.exceptions.RequestException: On unrecoverable HTTP/network error.
    """
    output_filepath = str(output_filepath)
    output_dir = os.path.dirname(output_filepath)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    logger.info("Starting CMS Medicare Part D data download...")

    total_rows = 0
    offset = 0
    first_batch = True

    while True:
        batch = _fetch_batch_with_retry(offset)

        if not batch:
            logger.info(f"Empty response at offset {offset}. Download complete.")
            break

        df_batch = pd.DataFrame(batch)
        df_batch.to_csv(
            output_filepath,
            mode="w" if first_batch else "a",
            header=first_batch,
            index=False,
        )
        first_batch = False
        total_rows += len(batch)
        logger.info(f"Wrote {len(batch)} records. Total so far: {total_rows}")

        # Last page: partial batch means no more data
        if len(batch) < BATCH_SIZE:
            logger.info("Partial batch received. Download complete.")
            break

        offset += BATCH_SIZE
        time.sleep(RATE_LIMIT_SLEEP)

    if total_rows == 0:
        raise ValueError("No records downloaded. Check API endpoint or dataset availability.")

    logger.info(f"Download complete. {total_rows} total rows saved to: {output_filepath}")
    return True
