"""
ETL pipeline functions for extracting, transforming, and loading review data.
"""

import pandas as pd


def extract_raw_data(sheet):
    """
    Extracts data from raw_data worksheet.
    
    Args:
        sheet: Google Sheet object
        
    Returns:
        pd.DataFrame: Raw review data
    """
    pass


def clean_data(df):
    """
    Cleans and standardizes review data.
    
    Args:
        df (pd.DataFrame): Raw data
        
    Returns:
        pd.DataFrame: Cleaned data
    """
    pass


def load_to_staging(sheet, df):
    """
    Loads cleaned data to staging worksheet.
    
    Args:
        sheet: Google Sheet object
        df (pd.DataFrame): Cleaned data
    """
    pass