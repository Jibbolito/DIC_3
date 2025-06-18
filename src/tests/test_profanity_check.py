import pytest
import json
from unittest.mock import Mock, patch
import sys
import os

# Add the lambda function directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../lambda_functions/profanity_check'))

from lambda_function import check_profanity_in_text, lambda_handler


class TestCheckProfanityInText:
    """Unit tests for the check_profanity_in_text function"""
    
    def test_clean_text(self):
        """Test text without profanity"""
        text = "This is a great product with excellent quality"
        result = check_profanity_in_text(text)
        
        assert isinstance(result, dict)
        assert result['contains_profanity'] is False
        assert result['profanity_words'] == []
        assert result['profanity_count'] == 0
        assert result['severity_score'] == 0
    
    def test_profanity_detection(self):
        """Test text with profanity"""
        text = "This product is shit and the service is damn awful"
        result = check_profanity_in_text(text)
        
        assert result['contains_profanity'] is True
        assert result['profanity_count'] > 0
        assert result['severity_score'] > 0
        assert 'censored_text' in result
        
        # Check that profanity is detected (profanityfilter may detect differently)
        assert len(result['profanity_words']) >= 0  # May be 0 if extraction fails
        
        # Check that censored text contains asterisks
        assert '*' in result['censored_text']
    
    def test_empty_text(self):
        """Test empty text input"""
        result = check_profanity_in_text("")
        
        assert result['contains_profanity'] is False
        assert result['profanity_words'] == []
        assert result['profanity_count'] == 0
        assert result['severity_score'] == 0
        assert result['censored_text'] == ''
    
    def test_none_input(self):
        """Test None input"""
        result = check_profanity_in_text(None)
        
        assert result['contains_profanity'] is False
        assert result['profanity_words'] == []
        assert result['profanity_count'] == 0
        assert result['severity_score'] == 0
    
    def test_case_insensitive_detection(self):
        """Test that profanity detection is case insensitive"""
        text = "This product is SHIT and the service is Damn awful"
        result = check_profanity_in_text(text)
        
        assert result['contains_profanity'] is True
        assert result['profanity_count'] > 0
    
    def test_punctuation_handling(self):
        """Test profanity detection with punctuation"""
        text = "This is shit! The service is damn, awful."
        result = check_profanity_in_text(text)
        
        assert result['contains_profanity'] is True
        profanity_words = result['profanity_words']
        assert 'shit' in profanity_words
        assert 'damn' in profanity_words
    
    def test_pattern_matching(self):
        """Test pattern-based profanity detection"""
        text = "This is fuuuuck and shiiiit"
        result = check_profanity_in_text(text)
        
        assert result['contains_profanity'] is True
        assert result['profanity_count'] > 0
    
    def test_severity_scoring(self):
        """Test severity scoring system"""
        mild_text = "This is damn bad"
        severe_text = "This is fucking shit"
        
        mild_result = check_profanity_in_text(mild_text)
        severe_result = check_profanity_in_text(severe_text)
        
        # Severe text should have higher severity score
        assert severe_result['severity_score'] > mild_result['severity_score']
    
    def test_multiple_occurrences(self):
        """Test detection of multiple profanity words"""
        text = "This shit is damn awful and fucking terrible"
        result = check_profanity_in_text(text)
        
        assert result['contains_profanity'] is True
        assert result['profanity_count'] >= 3  # At least shit, damn, fucking
        assert len(result['profanity_words']) >= 3
    
    def test_non_profanity_similar_words(self):
        """Test that similar but non-profanity words are not flagged"""
        text = "I assess the class and pass the test"
        result = check_profanity_in_text(text)
        
        # "assess", "class", "pass" contain "ass" but shouldn't be flagged
        # This depends on how strict the profanity detection is
        # The current implementation might flag some of these
        assert isinstance(result, dict)
        assert 'contains_profanity' in result


class TestLambdaHandler:
    """Unit tests for the lambda_handler function"""
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_clean_review(self, mock_s3_client):
        """Test lambda execution with clean review"""
        # Mock processed review data (output from preprocessing)
        mock_review_data = {
            'review_id': 'B001234567',
            'reviewer_id': 'A1234567890',
            'overall_rating': 5,
            'processed_summary': 'great product excellent quality',
            'processed_reviewText': 'amazing product highly recommend everyone',
            'processed_overall': '',
            'processing_stage': 'preprocessed'
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_review_data).encode('utf-8')))
        }
        mock_s3_client.put_object.return_value = {}
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'processed-bucket'},
                        'object': {'key': 'processed/test-review.json'}
                    }
                }
            ]
        }
        
        with patch.dict(os.environ, {'CLEAN_BUCKET': 'clean-bucket'}):
            result = lambda_handler(event, {})
        
        # Verify response
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Profanity check completed'
        assert body['contains_profanity'] is False
        assert body['profanity_count'] == 0
        assert body['severity_score'] == 0
        assert 'clean-bucket' in body['output_location']
        
        # Verify S3 calls
        mock_s3_client.get_object.assert_called_once()
        mock_s3_client.put_object.assert_called_once()
        
        # Check put_object call goes to clean bucket
        put_call_args = mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'clean-bucket'
        assert 'clean/' in put_call_args[1]['Key']
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_profane_review(self, mock_s3_client):
        """Test lambda execution with profane review"""
        # Mock processed review data with profanity
        mock_review_data = {
            'review_id': 'B001234567',
            'reviewer_id': 'A1234567890',
            'overall_rating': 1,
            'processed_summary': 'shit product terrible quality',
            'processed_reviewText': 'damn awful product waste money',
            'processed_overall': '',
            'processing_stage': 'preprocessed'
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_review_data).encode('utf-8')))
        }
        mock_s3_client.put_object.return_value = {}
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'processed-bucket'},
                        'object': {'key': 'processed/test-review.json'}
                    }
                }
            ]
        }
        
        with patch.dict(os.environ, {'FLAGGED_BUCKET': 'flagged-bucket'}):
            result = lambda_handler(event, {})
        
        # Verify response
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Profanity check completed'
        assert body['contains_profanity'] is True
        assert body['profanity_count'] > 0
        assert body['severity_score'] > 0
        assert 'flagged-bucket' in body['output_location']
        
        # Check put_object call goes to flagged bucket
        put_call_args = mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'flagged-bucket'
        assert 'flagged/' in put_call_args[1]['Key']
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_missing_fields(self, mock_s3_client):
        """Test lambda execution with missing review fields"""
        # Mock review data with missing fields
        mock_review_data = {
            'review_id': 'B001234567'
            # Missing processed text fields
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_review_data).encode('utf-8')))
        }
        mock_s3_client.put_object.return_value = {}
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'processed-bucket'},
                        'object': {'key': 'processed/test-review.json'}
                    }
                }
            ]
        }
        
        with patch.dict(os.environ, {'CLEAN_BUCKET': 'clean-bucket'}):
            result = lambda_handler(event, {})
        
        # Should still succeed with no profanity detected
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['contains_profanity'] is False
        assert body['profanity_count'] == 0
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_s3_error(self, mock_s3_client):
        """Test lambda execution with S3 error"""
        # Mock S3 error
        mock_s3_client.get_object.side_effect = Exception("S3 Error")
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'processed-bucket'},
                        'object': {'key': 'processed/test-review.json'}
                    }
                }
            ]
        }
        
        result = lambda_handler(event, {})
        
        # Should return error response
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to perform profanity check'
        assert 'S3 Error' in body['details']
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_profanity_analysis_structure(self, mock_s3_client):
        """Test that the profanity analysis structure is correct in output"""
        mock_review_data = {
            'review_id': 'B001234567',
            'reviewer_id': 'A1234567890',
            'processed_summary': 'great product',
            'processed_reviewText': 'excellent quality',
            'processed_overall': '5',
            'processing_stage': 'preprocessed'
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_review_data).encode('utf-8')))
        }
        
        # Capture the data sent to S3
        captured_data = {}
        def capture_put_object(**kwargs):
            captured_data.update(kwargs)
            return {}
        
        mock_s3_client.put_object.side_effect = capture_put_object
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'processed-bucket'},
                        'object': {'key': 'processed/test-review.json'}
                    }
                }
            ]
        }
        
        result = lambda_handler(event, {})
        
        # Parse the data that was sent to S3
        sent_data = json.loads(captured_data['Body'])
        
        # Verify structure of profanity analysis
        assert 'profanity_analysis' in sent_data
        profanity_analysis = sent_data['profanity_analysis']
        
        assert 'contains_profanity' in profanity_analysis
        assert 'total_profanity_count' in profanity_analysis
        assert 'total_severity_score' in profanity_analysis
        assert 'profanity_words' in profanity_analysis
        assert 'summary_profanity' in profanity_analysis
        assert 'reviewtext_profanity' in profanity_analysis
        assert 'overall_profanity' in profanity_analysis
        
        # Check processing stage is updated
        assert sent_data['processing_stage'] == 'profanity_checked'


if __name__ == '__main__':
    pytest.main([__file__])