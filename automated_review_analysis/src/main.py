from src.utils import connect_to_google_sheets, call_groq_llm
from src.etl import extract_raw_data, clean_data, load_to_staging


def run_etl_pipeline(spreadsheet):
    """  Runs the complete ETL pipeline: Extract → Clean → Load to Staging.  """

    raw_df = extract_raw_data(spreadsheet)
    if raw_df is None:
        print("❌ ETL Pipeline failed at extraction step")
        return None
    
    clean_df = clean_data(raw_df)
    if clean_df is None:
        print("❌ ETL Pipeline failed at cleaning step")
        return None
    
    success = load_to_staging(spreadsheet, clean_df)
    if not success:
        print("❌ ETL Pipeline failed at loading step")
        return None
   
    return clean_df


def run_full_pipeline():
    """  Runs the complete analysis pipeline: """
   
    print("\n Connecting to Google Sheets...")
    spreadsheet = connect_to_google_sheets()
    
    if not spreadsheet:
        print(" Pipeline failed: Could not connect to Google Sheets")
        return False
    
    print("\n Running ETL Pipeline...")
    cleaned_data = run_etl_pipeline(spreadsheet)
    
    if cleaned_data is None:
        print(" Pipeline failed: ETL process encountered errors")
        return False
    
    print("\n LLM Processing...")
    print("Skipping for now (will implement in Phase 6)")
    
    print("\n Analytics...")
    print(" Skipping for now (will implement in Phase 8)")
    
    print("✅ PIPELINE COMPLETED!")
    print(f"\n Processed {len(cleaned_data)} reviews")
    print(f" Check your Google Sheet: {spreadsheet.url}")
    
    return True


if __name__ == "__main__":
    run_full_pipeline()