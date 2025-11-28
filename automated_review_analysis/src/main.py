from src.utils import (connect_to_google_sheets, call_groq_llm)
from src.etl import (
    extract_raw_data, 
    clean_data, 
    load_to_staging,
    process_reviews_with_llm,
    load_to_processed
)
from src.analysis import (
    calculate_sentiment_breakdown,
    identify_top_classes,
    create_visualizations,
    generate_insights_report
)
def run_etl_pipeline(spreadsheet):
    """  Runs the complete ETL pipeline: Extract → Clean → Load to Staging.  """
    raw_df = extract_raw_data(spreadsheet)
    if raw_df is None:
        print("ETL Pipeline failed at extraction step")
        return None
    
    clean_df = clean_data(raw_df)
    if clean_df is None:
        print("ETL Pipeline failed at cleaning step")
        return None
    
    success = load_to_staging(spreadsheet, clean_df)
    if not success:
        print("ETL Pipeline failed at loading step")
        return None
   
    return clean_df

    
def run_llm_pipeline(spreadsheet, cleaned_data):
    """  Runs the LLM processing pipeline  """
    processed_df = process_reviews_with_llm(cleaned_data)
    if processed_df is None:
        print("LLM Pipeline failed at processing step")
        return None
    
    success = load_to_processed(spreadsheet, processed_df)
    if not success:
        print("LLM Pipeline failed at loading step")
        return None
    
    print("LLM PROCESSING PIPELINE COMPLETED SUCCESSFULLY!")
    
    print(f"\n Processed {len(cleaned_data)} reviews")

    return processed_df

def run_analysis_pipeline(processed_data):
    """
    Runs the analysis pipeline: Calculate metrics → Visualize → Generate report.
    """

    class_column = None
    for col in processed_data.columns:
        if isinstance(col, str) and 'class' in col.lower():
            class_column = col
            break

    if not class_column:
        print(" Could not find clothing class column || Skipping class-based analysis")
        return False
    
    print(f"📋 Using column '{class_column}' for class analysis")
        
    try: 
        breakdown, breakdown_pct = calculate_sentiment_breakdown(processed_data, class_column)
        if breakdown is None:
            print(" Analysis Pipeline failed at calculation step")
            return False
        
        top_classes = identify_top_classes(processed_data, breakdown_pct, class_column)
        charts = create_visualizations(processed_data, breakdown, breakdown_pct, class_column)
        if not charts:
            print("Visualization creation encountered issues")
        
        report = generate_insights_report(processed_data, breakdown, breakdown_pct, top_classes, class_column)

        return True
    except Exception as e:
        print(f" Error in analysis pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False
    
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
    
    print("\n Running LLM Processing Pipeline...")
    processed_data = run_llm_pipeline(spreadsheet, cleaned_data)
    
    if processed_data is None:
        print(" Pipeline failed: LLM processing encountered errors")
        return False
    
    print("\n Running Analysis Pipeline...")
    success = run_analysis_pipeline(processed_data)
    
    if not success:
        print("⚠️  Analysis completed with warnings")

    print("\n🎉 FULL PIPELINE COMPLETED SUCCESSFULLY!")
if __name__ == "__main__":

    run_full_pipeline()