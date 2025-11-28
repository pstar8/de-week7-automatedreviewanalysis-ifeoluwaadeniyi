import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from groq import Groq

# Load environment variables
load_dotenv()

def connect_to_google_sheets():
    """
    Establishes connection to Google Sheets
    """
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Load credentials 
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            'service_account.json', 
            scope
        )
        
        client = gspread.authorize(creds)
        
        sheet_id = os.getenv('GOOGLE_SHEET_ID')
        
        if not sheet_id:
            raise ValueError("GOOGLE_SHEET_ID not found in .env file")
        
        spreadsheet = client.open_by_key(sheet_id)
        
        print(f"✅ Successfully connected to: {spreadsheet.title}")
        return spreadsheet
    
    except FileNotFoundError:
        print("Error: service_account.json not found!")
        print("Make sure the file is in your project root directory.")
        return None
    
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return None

def call_groq_llm(review_text):
    """ Sends review text to Groq LLM for sentiment analysis   """
    if not review_text or str(review_text).strip() == '' or str(review_text).lower() == 'nan':
        return {
            'sentiment': 'Neutral',
            'summary': 'No review text provided'
        }
    
    try:
        api_key = os.getenv('GROQ_API_KEY')
        
        if not api_key:
            raise ValueError("GROQ_API_KEY not found in .env file")
        
        # Initialize Groq client
        client = Groq(api_key=api_key)
        
        prompt = f"""Analyze the following product review and provide:
                    1. Sentiment: Classify as either "Positive", "Negative", or "Neutral"
                    2. Summary: Provide a one-sentence summary of the review.

        Review: "{review_text}"

        Respond in this exact format:
        Sentiment: [Positive/Negative/Neutral]
        Summary: [Your one-sentence summary]"""
        
        # Call Groq API
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            model="llama-3.3-70b-versatile",  
            temperature=0.3, 
            max_tokens=150,
        )
        
        # Extract response
        response_text = chat_completion.choices[0].message.content.strip()
        
        # Parse the response
        sentiment = 'Neutral'  
        summary = review_text[:100] 
        lines = response_text.split('\n')
        for line in lines:
            if line.startswith('Sentiment:'):
                sentiment_raw = line.replace('Sentiment:', '').strip()

                if 'positive' in sentiment_raw.lower():
                    sentiment = 'Positive'
                elif 'negative' in sentiment_raw.lower():
                    sentiment = 'Negative'
                else:
                    sentiment = 'Neutral'
            
            elif line.startswith('Summary:'):
                summary = line.replace('Summary:', '').strip()
        
        return {
            'sentiment': sentiment,
            'summary': summary
        }
    
    except Exception as e:
        print(f"⚠️  Error calling Groq LLM: {e}")
        return {
            'sentiment': 'Neutral',
            'summary': f'Error processing review: {str(e)[:50]}'
        }