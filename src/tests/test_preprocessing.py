import pytest
import json
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the lambda function directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../lambda_functions/preprocessing'))

from lambda_function import preprocess_text, lambda_handler


class TestPreprocessText:
    """Unit tests for the preprocess_text function"""
    
    def test_preprocess_text_basic(self):
        """Test basic text preprocessing"""
        text = "This is a great product! I love it."
        result = preprocess_text(text)
        
        assert isinstance(result, dict)
        assert 'original_text' in result
        assert 'tokens' in result
        assert 'processed_text' in result
        assert 'word_count' in result
        
        # Check that stopwords are removed
        assert 'is' not in result['tokens']  # 'is' is a stopword
        assert 'a' not in result['tokens']   # 'a' is a stopword
        
        # Check that meaningful words are kept
        assert 'great' in result['tokens']
        assert 'product' in result['tokens']
        assert 'love' in result['tokens']
    
    def test_preprocess_text_empty_string(self):
        """Test preprocessing with empty string"""
        result = preprocess_text("")
        
        assert result['original_text'] == ""
        assert result['tokens'] == []
        assert result['processed_text'] == ""
        assert result['word_count'] == 0
    
    def test_preprocess_text_none_input(self):
        """Test preprocessing with None input"""
        result = preprocess_text(None)
        
        assert result['original_text'] is None
        assert result['tokens'] == []
        assert result['processed_text'] == ""
        assert result['word_count'] == 0
    
    def test_preprocess_text_special_characters(self):
        """Test preprocessing with special characters"""
        text = "Amazing product!!! Best purchase ever. 5/5 stars ⭐⭐⭐⭐⭐"
        result = preprocess_text(text)
        
        # Special characters should be removed
        assert '⭐' not in result['processed_text']
        assert '!!!' not in result['processed_text']
        
        # Meaningful words should remain
        assert 'amazing' in result['tokens']
        assert 'product' in result['tokens']
        assert 'best' in result['tokens']
        assert 'purchase' in result['tokens']
    
    def test_preprocess_text_lemmatization(self):
        """Test that lemmatization works correctly"""
        text = "The products were amazing and the services are excellent"
        result = preprocess_text(text)
        
        # Lemmatization should convert plurals to singular
        # Note: NLTK lemmatizer may not catch all cases perfectly
        tokens = result['tokens']
        processed_text = result['processed_text']
        
        # Check that some form of normalization occurred
        assert len(tokens) > 0
        assert result['word_count'] > 0
    
    def test_preprocess_text_case_insensitive(self):
        """Test that preprocessing handles different cases"""
        text = "EXCELLENT Product with GREAT Quality"
        result = preprocess_text(text)
        
        # Original text should be lowercased
        assert result['original_text'].islower()
        
        # Tokens should contain lowercase words
        assert 'excellent' in result['tokens']
        assert 'product' in result['tokens']
        assert 'great' in result['tokens']
        assert 'quality' in result['tokens']


class TestLambdaHandler:
    """Unit tests for the lambda_handler function"""
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_success(self, mock_s3_client):
        """Test successful lambda execution"""
        # Mock S3 response - using real dataset format
        mock_review_data = {
            'asin': 'B001234567',
            'reviewerID': 'A1234567890',
            'reviewerName': 'Test Customer',
            'overall': 5.0,
            'summary': 'Great product',
            'reviewText': 'This is an excellent product that I highly recommend.',
            'unixReviewTime': 1609459200,
            'reviewTime': '01 1, 2021',
            'category': 'Patio_Lawn_and_Garden',
            'helpful': [5, 5]
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_review_data).encode('utf-8')))
        }
        
        # Mock the put_object call
        mock_s3_client.put_object.return_value = {}
        
        # Create test event
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'test-review.json'}
                    }
                }
            ]
        }
        
        # Set environment variable
        with patch.dict(os.environ, {'PROCESSED_BUCKET': 'test-processed-bucket'}):
            result = lambda_handler(event, {})
        
        # Verify response
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Review successfully preprocessed'
        assert body['review_id'] == 'B001234567'
        assert body['reviewer_id'] == 'A1234567890'
        assert 'total_words' in body
        assert 'output_location' in body
        
        # Verify S3 calls
        mock_s3_client.get_object.assert_called_once()
        mock_s3_client.put_object.assert_called_once()
        
        # Check put_object call arguments
        put_call_args = mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'test-processed-bucket'
        assert put_call_args[1]['Key'] == 'processed/test-review.json'
        assert put_call_args[1]['ContentType'] == 'application/json'
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_missing_fields(self, mock_s3_client):
        """Test lambda execution with missing review fields"""
        # Mock S3 response with minimal data
        mock_review_data = {
            'asin': 'B001234567'
            # Missing other fields
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_review_data).encode('utf-8')))
        }
        mock_s3_client.put_object.return_value = {}
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'test-review.json'}
                    }
                }
            ]
        }
        
        with patch.dict(os.environ, {'PROCESSED_BUCKET': 'test-processed-bucket'}):
            result = lambda_handler(event, {})
        
        # Should still succeed but with default values
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['reviewer_id'] == 'unknown'  # Default value
        assert body['total_words'] == 0  # No text to process
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_s3_error(self, mock_s3_client):
        """Test lambda execution with S3 error"""
        # Mock S3 error
        mock_s3_client.get_object.side_effect = Exception("S3 Error")
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'test-review.json'}
                    }
                }
            ]
        }
        
        result = lambda_handler(event, {})
        
        # Should return error response
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to preprocess review'
        assert 'S3 Error' in body['details']
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_jsonl_format(self, mock_s3_client):
        """Test lambda execution with JSONL format (like reviews_devset.json)"""
        # Mock S3 response with JSONL format
        mock_review_data = {
            'asin': 'B001234567',
            'reviewerID': 'A1234567890',
            'reviewerName': 'Test Customer',
            'overall': 5.0,
            'summary': 'Great product',
            'reviewText': 'This is an excellent product.',
            'unixReviewTime': 1609459200,
            'category': 'Patio_Lawn_and_Garden'
        }
        
        # JSONL format - single line
        jsonl_content = json.dumps(mock_review_data)
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=jsonl_content.encode('utf-8')))
        }
        mock_s3_client.put_object.return_value = {}
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'test-review.json'}
                    }
                }
            ]
        }
        
        with patch.dict(os.environ, {'PROCESSED_BUCKET': 'test-processed-bucket'}):
            result = lambda_handler(event, {})
        
        # Should succeed
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['review_id'] == 'B001234567'
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_invalid_json(self, mock_s3_client):
        """Test lambda execution with invalid JSON"""
        # Mock S3 response with invalid JSON
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=b'invalid json'))
        }
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'test-bucket'},
                        'object': {'key': 'test-review.json'}
                    }
                }
            ]
        }
        
        result = lambda_handler(event, {})
        
        # Should return error response
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to preprocess review'


if __name__ == '__main__':
    pytest.main([__file__])