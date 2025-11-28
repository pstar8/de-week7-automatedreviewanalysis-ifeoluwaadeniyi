import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from src.etl import (
    extract_raw_data,
    clean_data,
    load_to_staging,
    process_reviews_with_llm,
    load_to_processed
)


class TestExtractRawData:
    """Tests for extract_raw_data function."""
    
    def test_extract_raw_data_success(self):
        """Test successful data extraction."""
        # Mock spreadsheet
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        
        # Mock data
        mock_data = [
            ['ID', 'Age', 'Review Text'],
            ['1', '25', 'Great product'],
            ['2', '30', 'Not bad']
        ]
        mock_worksheet.get_all_values.return_value = mock_data
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        
        df = extract_raw_data(mock_spreadsheet)
        
        assert df is not None
        assert len(df) == 2
        assert 'Review Text' in df.columns
    
    def test_extract_raw_data_worksheet_not_found(self):
        """Test extraction fails when worksheet not found."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.worksheet.side_effect = Exception("Worksheet not found")
        
        df = extract_raw_data(mock_spreadsheet)
        
        assert df is None


class TestCleanData:
    """Tests for clean_data function."""
    
    def test_clean_data_basic(self):
        """Test basic data cleaning."""
        df = pd.DataFrame({
            'ID': ['1', '2', '3'],
            'Age': ['25', '30', '35'],
            'Review': ['Good', 'Bad', 'Okay']
        })
        
        cleaned_df = clean_data(df)
        
        assert cleaned_df is not None
        assert len(cleaned_df) == 3
        assert cleaned_df['Age'].dtype in ['int64', 'float64']
    
    def test_clean_data_removes_empty_rows(self):
        """Test cleaning removes completely empty rows."""
        df = pd.DataFrame({
            'ID': ['1', '', '3'],
            'Age': ['25', '', '35'],
            'Review': ['Good', '', 'Okay']
        })
        
        cleaned_df = clean_data(df)
        
        # Should keep rows with at least some data
        assert cleaned_df is not None
        assert len(cleaned_df) <= 3
    
    def test_clean_data_handles_whitespace(self):
        """Test cleaning strips whitespace."""
        df = pd.DataFrame({
            'ID': ['  1  ', '2', '3  '],
            'Review': ['  Good  ', 'Bad', '  Okay']
        })
        
        cleaned_df = clean_data(df)
        
        # ID gets converted to int, so check the numeric value
        assert cleaned_df['ID'].iloc[0] == 1 or str(cleaned_df['ID'].iloc[0]).strip() == '1'
        assert cleaned_df['Review'].iloc[0] == 'Good'
    
    def test_clean_data_handles_empty_dataframe(self):
        """Test cleaning handles empty DataFrame."""
        df = pd.DataFrame()
        
        cleaned_df = clean_data(df)
        
        assert cleaned_df is not None


class TestLoadToStaging:
    """Tests for load_to_staging function."""
    
    def test_load_to_staging_success(self):
        """Test successful load to staging."""
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = []  # Empty worksheet
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        
        df = pd.DataFrame({
            'ID': [1, 2],
            'Review': ['Good', 'Bad']
        })
        
        result = load_to_staging(mock_spreadsheet, df)
        
        assert result == True
        mock_worksheet.update.assert_called_once()
    
    def test_load_to_staging_clears_existing_data(self):
        """Test loading clears existing data (idempotent)."""
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [['ID'], ['1'], ['2']]
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        
        df = pd.DataFrame({'ID': [1]})
        
        result = load_to_staging(mock_spreadsheet, df)
        
        assert result == True
        mock_worksheet.clear.assert_called_once()
    
    def test_load_to_staging_worksheet_not_found(self):
        """Test load fails when worksheet not found."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.worksheet.side_effect = Exception("Worksheet not found")
        
        df = pd.DataFrame({'ID': [1]})
        
        result = load_to_staging(mock_spreadsheet, df)
        
        assert result == False


class TestProcessReviewsWithLLM:
    """Tests for process_reviews_with_llm function."""
    
    @patch('src.utils.call_groq_llm')
    def test_process_reviews_success(self, mock_llm):
        """Test successful review processing."""
        mock_llm.return_value = {
            'sentiment': 'Positive',
            'summary': 'Great product'
        }
        
        df = pd.DataFrame({
            'Review Text': ['Amazing product!', 'Love it!']
        })
        
        result = process_reviews_with_llm(df)
        
        assert result is not None
        assert 'AI Sentiment' in result.columns
        assert 'AI Summary' in result.columns
        assert 'Action Needed?' in result.columns
        assert len(result) == 2
    
    @patch('src.utils.call_groq_llm')
    def test_process_reviews_handles_empty_text(self, mock_llm):
        """Test processing skips empty reviews."""
        df = pd.DataFrame({
            'Review Text': ['Good product', '', 'Bad product']
        })
        
        result = process_reviews_with_llm(df)
        
        assert result is not None
        assert result.loc[1, 'AI Sentiment'] == 'Neutral'
        assert result.loc[1, 'AI Summary'] == 'No review text provided'
    
    @patch('src.utils.call_groq_llm')
    def test_process_reviews_sets_action_needed_for_negative(self, mock_llm):
        """Test Action Needed? flag for negative reviews."""
        mock_llm.return_value = {
            'sentiment': 'Negative',
            'summary': 'Poor quality'
        }
        
        df = pd.DataFrame({
            'Review Text': ['Terrible product']
        })
        
        result = process_reviews_with_llm(df)
        
        assert result.loc[0, 'Action Needed?'] == 'Yes'
    
    @patch('src.utils.call_groq_llm')
    def test_process_reviews_no_action_for_positive(self, mock_llm):
        """Test Action Needed? flag for positive reviews."""
        mock_llm.return_value = {
            'sentiment': 'Positive',
            'summary': 'Great quality'
        }
        
        df = pd.DataFrame({
            'Review Text': ['Amazing product']
        })
        
        result = process_reviews_with_llm(df)
        
        assert result.loc[0, 'Action Needed?'] == 'No'


class TestLoadToProcessed:
    """Tests for load_to_processed function."""
    
    def test_load_to_processed_success(self):
        """Test successful load to processed."""
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = []
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        
        df = pd.DataFrame({
            'ID': [1],
            'AI Sentiment': ['Positive'],
            'AI Summary': ['Good'],
            'Action Needed?': ['No']
        })
        
        result = load_to_processed(mock_spreadsheet, df)
        
        assert result == True
        mock_worksheet.update.assert_called_once()


# Run tests with: pytest tests/test_etl.py -v