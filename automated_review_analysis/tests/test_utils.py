import pytest
import os
from unittest.mock import patch, MagicMock
from src.utils import connect_to_google_sheets, call_groq_llm


class TestGoogleSheetsConnection:
    """Tests for Google Sheets connection function."""
    
    def test_connect_to_google_sheets_success(self):
        if not os.path.exists('service_account.json'):
            pytest.skip("service_account.json not found")
        
        spreadsheet = connect_to_google_sheets()
        assert spreadsheet is not None
        assert hasattr(spreadsheet, 'title')
        assert hasattr(spreadsheet, 'url')
    
    @patch.dict(os.environ, {'GOOGLE_SHEET_ID': ''})
    def test_connect_without_sheet_id(self):
        """Test connection fails without GOOGLE_SHEET_ID."""
        result = connect_to_google_sheets()
        assert result is None
    
    @patch('src.utils.ServiceAccountCredentials.from_json_keyfile_name')
    def test_connect_missing_credentials_file(self, mock_creds):
        """Test connection fails with missing credentials file."""
        mock_creds.side_effect = FileNotFoundError("service_account.json not found")
        
        result = connect_to_google_sheets()
        assert result is None


class TestGroqLLM:
    """Tests for Groq LLM integration."""
    
    def test_call_groq_llm_empty_review(self):
        """Test LLM handles empty review text."""
        result = call_groq_llm("")
        
        assert result is not None
        assert result['sentiment'] == 'Neutral'
        assert result['summary'] == 'No review text provided'
    
    def test_call_groq_llm_none_review(self):
        """Test LLM handles None review text."""
        result = call_groq_llm(None)
        
        assert result is not None
        assert result['sentiment'] == 'Neutral'
        assert result['summary'] == 'No review text provided'
    
    def test_call_groq_llm_nan_review(self):
        """Test LLM handles 'nan' string."""
        result = call_groq_llm('nan')
        
        assert result is not None
        assert result['sentiment'] == 'Neutral'
        assert result['summary'] == 'No review text provided'
    
    @patch('src.utils.Groq')
    def test_call_groq_llm_positive_sentiment(self, mock_groq):
        """Test LLM correctly identifies positive sentiment."""
        # Mock the Groq API response
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = """SENTIMENT: Positive
SUMMARY: Customer loves the product and highly recommends it."""
        
        mock_client.chat.completions.create.return_value = mock_response
        mock_groq.return_value = mock_client
        
        result = call_groq_llm("This product is amazing! I love it!")
        
        assert result is not None
        assert result['sentiment'] == 'Positive'
        assert len(result['summary']) > 0
    
    @patch('src.utils.Groq')
    def test_call_groq_llm_negative_sentiment(self, mock_groq):
        """Test LLM correctly identifies negative sentiment."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = """SENTIMENT: Negative
SUMMARY: Customer is disappointed with poor quality."""
        
        mock_client.chat.completions.create.return_value = mock_response
        mock_groq.return_value = mock_client
        
        result = call_groq_llm("Terrible quality, very disappointed.")
        
        assert result is not None
        assert result['sentiment'] == 'Negative'
        assert len(result['summary']) > 0
    
    @patch('src.utils.Groq')
    def test_call_groq_llm_api_error(self, mock_groq):
        """Test LLM handles API errors gracefully."""
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = Exception("API Error")
        mock_groq.return_value = mock_client
        
        result = call_groq_llm("Some review text")
        
        assert result is not None
        assert result['sentiment'] == 'Neutral'
        assert 'Error' in result['summary']
    
    @patch.dict(os.environ, {'GROQ_API_KEY': ''})
    def test_call_groq_llm_missing_api_key(self):
        """Test LLM handles missing API key."""
        result = call_groq_llm("Some review text")
        
        assert result is not None
        assert result['sentiment'] == 'Neutral'

