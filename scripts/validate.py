import pandas as pd
import logging

def validate_data(filepath):
    logging.info("Starting data validation...")
    
    df = pd.read_csv(filepath, low_memory=False)
    
    # Check 1: Empty file
    if len(df) == 0:
        raise ValueError("VALIDATION FAILED: File is empty.")
    logging.info(f"Total rows found: {len(df)}")
    
    # Check 2: Required columns
    required_columns = ['PRSCRBR_NPI', 'Prscrbr_Last_Org_Name', 
                        'Tot_Clms', 'Tot_Drug_Cst']
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"VALIDATION FAILED: Missing columns: {missing}")
    logging.info("All required columns present.")
    
    # Check 3: Null counts
    null_counts = df[required_columns].isnull().sum().to_dict()
    logging.info(f"Null counts: {null_counts}")
    
    # Check 4: No negative values
    if (df['Tot_Clms'] < 0).any():
        raise ValueError("VALIDATION FAILED: Negative values in Tot_Clms.")
    logging.info("No negative values found.")
    
    # Check 5: Duplicate check
    duplicate_count = df['PRSCRBR_NPI'].duplicated().sum()
    logging.info(f"Duplicate NPIs found: {duplicate_count}")
    
    logging.info("Validation passed successfully.")
    return True