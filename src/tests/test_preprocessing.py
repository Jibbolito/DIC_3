import pytest
import json
from unittest.mock import Mock, patch, MagicMock
import sys
import os


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
        
        
        assert 'is' not in result['tokens']  
        assert 'a' not in result['tokens']   
        
        
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
        
        
        assert '⭐' not in result['processed_text']
        assert '!!!' not in result['processed_text']
        
        
        assert 'amazing' in result['tokens']
        assert 'product' in result['tokens']
        assert 'best' in result['tokens']
        assert 'purchase' in result['tokens']
    
    def test_preprocess_text_lemmatization(self):
        """Test that lemmatization works correctly"""
        text = "The products were amazing and the services are excellent"
        result = preprocess_text(text)
        
        
        
        tokens = result['tokens']
        
        
        assert len(tokens) > 0
        assert result['word_count'] > 0
    
    def test_preprocess_text_case_insensitive(self):
        """Test that preprocessing handles different cases"""
        text = "EXCELLENT Product with GREAT Quality"
        result = preprocess_text(text)
        
        
        assert result['original_text'].islower()
        
        
        assert 'excellent' in result['tokens']
        assert 'product' in result['tokens']
        assert 'great' in result['tokens']
        assert 'quality' in result['tokens']


class TestLambdaHandler:
    """Unit tests for the lambda_handler function"""
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_success(self, mock_s3_client):
        """Test successful lambda execution"""
        
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
        
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == '1 review(s) successfully preprocessed and sent to test-processed-bucket'
        assert 'processed_count' in body
        assert body['errors_count'] == 0
        
        
        mock_s3_client.get_object.assert_called_once()
        mock_s3_client.put_object.assert_called_once()
        
        
        put_call_args = mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'test-processed-bucket'
        assert put_call_args[1]['Key'] == 'processed/test-review/B001234567A1234567890.json'
        assert put_call_args[1]['ContentType'] == 'application/json'
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_missing_fields(self, mock_s3_client):
        """Test lambda execution with missing review fields"""
        
        mock_review_data = {
            'asin': 'B001234567'
            
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
        
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == '1 review(s) successfully preprocessed and sent to test-processed-bucket'
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_s3_error(self, mock_s3_client):
        """Test lambda execution with S3 error"""
        
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
        
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to preprocess reviews'
        assert 'S3 Error' in body['details']
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_jsonl_format(self, mock_s3_client):
        """Test lambda execution with JSONL format (like reviews_devset.json)"""
        
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
        
        
        jsonl_content = json.dumps(mock_review_data, indent=2) 
        
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
        
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == '1 review(s) successfully preprocessed and sent to test-processed-bucket'
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_invalid_json(self, mock_s3_client):
        """Test lambda execution with invalid JSON"""
        
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
        
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to preprocess reviews'


if __name__ == '__main__':
    pytest.main([__file__])