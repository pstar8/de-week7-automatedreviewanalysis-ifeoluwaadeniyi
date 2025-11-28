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
    """
    Sends review text to Groq LLM for sentiment analysis.
    """
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
        
        # Create a VERY clear prompt
        prompt = f"""You are analyzing a product review. Read it carefully and respond with EXACTLY this format:

                SENTIMENT: [Choose ONLY one: Positive OR Negative OR Neutral]
                SUMMARY: [Write one clear sentence summarizing the review]

                Review to analyze: "{review_text}"

                Remember:
                - Positive: Customer likes the product (good, great, love, recommend, etc.)
                - Negative: Customer dislikes it (bad, terrible, disappointed, poor quality, etc.)
                - Neutral: Mixed feelings or just describing facts

                Your response:"""
        
        # Call Groq API
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": "You are a sentiment analysis expert. Always respond in the exact format requested."
                },
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            model="openai/gpt-oss-20b",
            temperature=0.1,  # Very low for consistency
            max_tokens=150,
        )
        
        # Extract response
        response_text = chat_completion.choices[0].message.content.strip()
        
        # Parse the response more robustly
        sentiment = 'Neutral'  # Default
        summary = review_text[:100] if len(review_text) > 100 else review_text
        
        # Try to parse line by line
        lines = response_text.split('\n')
        for line in lines:
            line = line.strip()
            
            # Look for SENTIMENT line
            if line.upper().startswith('SENTIMENT:'):
                sentiment_raw = line.split(':', 1)[1].strip().lower()
                
                # More flexible matching
                if 'positive' in sentiment_raw:
                    sentiment = 'Positive'
                elif 'negative' in sentiment_raw:
                    sentiment = 'Negative'
                else:
                    sentiment = 'Neutral'
            
            # Look for SUMMARY line
            elif line.upper().startswith('SUMMARY:'):
                summary = line.split(':', 1)[1].strip()
        
        # Fallback: if response doesn't follow format, try to infer sentiment
        if sentiment == 'Neutral' and summary == review_text[:100]:
            response_lower = response_text.lower()
            if any(word in response_lower for word in ['positive', 'good', 'great', 'love', 'excellent']):
                sentiment = 'Positive'
            elif any(word in response_lower for word in ['negative', 'bad', 'poor', 'terrible', 'disappointed']):
                sentiment = 'Negative'
        
        return {
            'sentiment': sentiment,
            'summary': summary
        }
    
    except Exception as e:
        print(f"⚠️  Error calling Groq LLM: {e}")
        # Return neutral sentiment on error
        return {
            'sentiment': 'Neutral',
            'summary': f'Error processing review'
        }