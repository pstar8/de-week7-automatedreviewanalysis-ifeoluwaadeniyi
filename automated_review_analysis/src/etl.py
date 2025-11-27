import pandas as pd
import gspread


def extract_raw_data(spreadsheet):
    """ Extracts data from raw_data worksheet."""

    try:
        print("EXTRACTING DATA FROM raw_data")
        
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
    try:
        df_clean = df.copy()
        
        print("📊 Converting data types...")
        
        numeric_cols = []
        for col in df_clean.columns:
            # Convert to numeric
            try:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='ignore')
                if df_clean[col].dtype in ['int64', 'float64']:
                    numeric_cols.append(col)
            except:
                pass
        
        print(f"   Converted {len(numeric_cols)} numeric columns")

       #  Cleaning review text columns
        review_col = None
        for col in df_clean.columns:
            if 'review' in col.lower() and 'text' in col.lower():
                review_col = col
                break
        
        if review_col:
            df_clean[review_col] = df_clean[review_col].replace(['', 'nan', 'NaN', 'None'], pd.NA)
            
            missing_reviews = df_clean[review_col].isna().sum()
            print(f"   Found {missing_reviews} empty reviews")

         #Standardize text
        
        text_cols = df_clean.select_dtypes(include=['object']).columns
        for col in text_cols:
            df_clean[col] = df_clean[col].astype(str).str.strip()
            df_clean[col] = df_clean[col].replace('nan', '')
        
        # Remove completely empty rows 
        
        initial_rows = len(df_clean)
        df_clean = df_clean.dropna(how='all')
        removed_rows = initial_rows - len(df_clean)
        
        if removed_rows > 0:
            print(f"   Removed {removed_rows} completely empty rows")
        
        #Reset index
        df_clean = df_clean.reset_index(drop=True)
        
        print(f"\n Cleaning complete!")
        print(f"   Final dataset: {len(df_clean)} rows, {len(df_clean.columns)} columns")
        
        return df_clean

    except Exception as e:
        print(f"❌ Error cleaning data: {e}")
        return df


def load_to_staging(sheet, df):
    """
    Loads cleaned data to staging worksheet.
    
    Args:
        sheet: Google Sheet object
        df (pd.DataFrame): Cleaned data
    """
    pass