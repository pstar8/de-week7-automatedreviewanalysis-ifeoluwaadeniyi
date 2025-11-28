import pytest
import pandas as pd
import os
from unittest.mock import patch, MagicMock
from src.analysis import (
    calculate_sentiment_breakdown,
    identify_top_classes,
    create_visualizations,
    generate_insights_report
)


class TestCalculateSentimentBreakdown:
    """Tests for calculate_sentiment_breakdown function."""
    
    def test_calculate_breakdown_success(self):
        """Test successful sentiment breakdown calculation."""
        df = pd.DataFrame({
            'Class Name': ['Dresses', 'Dresses', 'Tops', 'Tops'],
            'AI Sentiment': ['Positive', 'Negative', 'Positive', 'Positive']
        })
        
        breakdown, breakdown_pct = calculate_sentiment_breakdown(df)
        
        assert breakdown is not None
        assert breakdown_pct is not None
        assert 'Dresses' in breakdown.index
        assert 'Tops' in breakdown.index
    
    def test_calculate_breakdown_missing_sentiment_column(self):
        """Test breakdown fails without AI Sentiment column."""
        df = pd.DataFrame({
            'Class Name': ['Dresses', 'Tops'],
            'Rating': [5, 3]
        })
        
        result = calculate_sentiment_breakdown(df)

        assert result is None or result == (None, None)
    
    def test_calculate_breakdown_missing_class_column(self):
        """Test breakdown fails without class column."""
        df = pd.DataFrame({
            'Product': ['A', 'B'],
            'AI Sentiment': ['Positive', 'Negative']
        })
        
        result = calculate_sentiment_breakdown(df)
        
        assert result is None or result == (None, None)


class TestIdentifyTopClasses:
    """Tests for identify_top_classes function."""
    
    def test_identify_top_classes_success(self):
        """Test successful identification of top classes."""
        df = pd.DataFrame({
            'Class Name': ['Dresses', 'Tops', 'Pants'],
            'AI Sentiment': ['Positive', 'Negative', 'Neutral']
        })
        
        breakdown_pct = pd.DataFrame({
            'Positive': [80.0, 20.0, 50.0],
            'Negative': [10.0, 70.0, 30.0],
            'Neutral': [10.0, 10.0, 20.0]
        }, index=['Dresses', 'Tops', 'Pants'])
        
        results = identify_top_classes(df, breakdown_pct)
        
        assert 'highest_positive' in results
        assert 'highest_negative' in results
        assert results['highest_positive']['class'] == 'Dresses'
        assert results['highest_negative']['class'] == 'Tops'
    
    def test_identify_top_classes_handles_errors(self):
        """Test top classes handles errors gracefully."""
        df = pd.DataFrame()
        breakdown_pct = pd.DataFrame()
        
        results = identify_top_classes(df, breakdown_pct)
        
        assert isinstance(results, dict)


class TestCreateVisualizations:
    """Tests for create_visualizations function."""
    
    @patch('src.analysis.plt.savefig')
    @patch('src.analysis.plt.close')
    def test_create_visualizations_success(self, mock_close, mock_savefig):
        """Test successful visualization creation."""
        df = pd.DataFrame({
            'Class Name': ['Dresses', 'Dresses', 'Tops', 'Tops'],
            'AI Sentiment': ['Positive', 'Negative', 'Positive', 'Positive']
        })
        
        breakdown = pd.DataFrame({
            'Positive': [1, 2],
            'Negative': [1, 0],
            'Neutral': [0, 0],
            'Total': [2, 2]
        }, index=['Dresses', 'Tops'])
        
        breakdown_pct = pd.DataFrame({
            'Positive': [50.0, 100.0],
            'Negative': [50.0, 0.0],
            'Neutral': [0.0, 0.0]
        }, index=['Dresses', 'Tops'])
        
        charts = create_visualizations(df, breakdown, breakdown_pct)
        
        assert isinstance(charts, list)
        # Should create 4 charts
        assert mock_savefig.call_count == 4
    
    def test_create_visualizations_creates_directory(self):
        """Test visualizations creates charts directory."""
        df = pd.DataFrame({
            'Class Name': ['Dresses'],
            'AI Sentiment': ['Positive']
        })
        
        breakdown = pd.DataFrame({
            'Positive': [1],
            'Negative': [0],
            'Neutral': [0]
        }, index=['Dresses'])
        
        breakdown_pct = breakdown.copy()
        
        # Clean up first
        if os.path.exists('charts'):
            import shutil
            shutil.rmtree('charts')
        
        create_visualizations(df, breakdown, breakdown_pct)
        
        assert os.path.exists('charts')


class TestGenerateInsightsReport:
    """Tests for generate_insights_report function."""
    
    def test_generate_insights_report_success(self):
        """Test successful report generation."""
        df = pd.DataFrame({
            'Class Name': ['Dresses', 'Dresses', 'Tops'],
            'AI Sentiment': ['Positive', 'Negative', 'Positive'],
            'Action Needed?': ['No', 'Yes', 'No']
        })
        
        breakdown = pd.DataFrame({
            'Positive': [1, 1],
            'Negative': [1, 0],
            'Total': [2, 1]
        }, index=['Dresses', 'Tops'])
        
        breakdown_pct = pd.DataFrame({
            'Positive': [50.0, 100.0],
            'Negative': [50.0, 0.0]
        }, index=['Dresses', 'Tops'])
        
        top_classes = {
            'highest_positive': {'class': 'Tops', 'percentage': 100.0},
            'highest_negative': {'class': 'Dresses', 'percentage': 50.0}
        }
        
        report = generate_insights_report(df, breakdown, breakdown_pct, top_classes)
        
        assert isinstance(report, str)
        assert len(report) > 0
        assert 'INSIGHTS REPORT' in report
        assert os.path.exists('insights_report.txt')
    
    def test_generate_insights_report_handles_high_positive(self):
        """Test report identifies high satisfaction."""
        df = pd.DataFrame({
            'Class Name': ['A'] * 10,
            'AI Sentiment': ['Positive'] * 7 + ['Negative'] * 3,
            'Action Needed?': ['No'] * 7 + ['Yes'] * 3
        })
        
        breakdown = pd.DataFrame({'Positive': [7], 'Negative': [3]}, index=['A'])
        breakdown_pct = pd.DataFrame({'Positive': [70.0], 'Negative': [30.0]}, index=['A'])
        top_classes = {}
        
        report = generate_insights_report(df, breakdown, breakdown_pct, top_classes)
        
        assert 'HIGH' in report or 'MODERATE' in report
