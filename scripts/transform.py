import pandas as pd
import logging
from datetime import datetime

def transform_data(input_filepath, output_filepath):
    logging.info("Starting data transformation...")
    
    # Read the raw CSV
    df = pd.read_csv(input_filepath, low_memory=False)

    # Step 1: Standardize column names to lowercase
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # Step 2: Remove duplicate NPIs
    before = len(df)
    df = df.drop_duplicates(subset=['prscrbr_npi'])
    after = len(df)
    logging.info(f"Removed {before - after} duplicate rows.")

    # Step 3: Fill missing values
    df['prscrbr_state_abrvtn'] = df['prscrbr_state_abrvtn'].fillna('UNKNOWN')
    df['tot_clms'] = df['tot_clms'].fillna(0)
    df['tot_drug_cst'] = df['tot_drug_cst'].fillna(0.0)

    # Step 4: Standardize state abbreviations
    df['prscrbr_state_abrvtn'] = df['prscrbr_state_abrvtn'].str.upper().str.strip()

    # Step 5: Create new metric - average cost per claim
    df['avg_cost_per_claim'] = (
        df['tot_drug_cst'] / df['tot_clms'].replace(0, 1)
    ).round(2)

    # Step 6: Add pipeline metadata
    df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['data_source'] = 'CMS_Medicare_PartD'

    # Step 7: Save clean output
    df.to_csv(output_filepath, index=False)
    logging.info(f"Clean data saved. Total rows: {len(df)}")
    
    return True