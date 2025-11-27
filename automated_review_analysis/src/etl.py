import pandas as pd
import gspread


def extract_raw_data(spreadsheet):
    """ Extracts data from raw_data worksheet."""

    try:
        print("📥 EXTRACTING DATA FROM raw_data")
        
        raw_worksheet = spreadsheet.worksheet('raw_data')
        raw_data = raw_worksheet.get_all_values()
        
        # Convert to DataFrame
        headers = raw_data[0]
        data_rows = raw_data[1:]
        
        df = pd.DataFrame(data_rows, columns=headers)
        
        print(f"✅ Extracted {len(df)} rows and {len(df.columns)} columns")
        print(f"   Columns: {', '.join(df.columns[:5])}...")
        
        return df
    
    except gspread.exceptions.WorksheetNotFound:
        print("Error: 'raw_data' worksheet not found!")
        print("Make sure you have a worksheet named exactly 'raw_data'")
        return None
    
    except Exception as e:
        print(f"Error extracting data: {e}")
        return None



def clean_data(df):
    """ Cleans and standardizes review data.  """
    pass


def load_to_staging(sheet, df):
    """
    Loads cleaned data to staging worksheet.
    
    Args:
        sheet: Google Sheet object
        df (pd.DataFrame): Cleaned data
    """
    pass