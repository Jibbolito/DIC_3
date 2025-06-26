import pytest
import json
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the lambda function directory to the path
# Assuming this test file is in 'tests/' and lambda functions are in 'src/'
sys.path.append(os.path.join(os.path.dirname(__file__), '../lambda_functions/preprocessing'))

from lambda_function import preprocess_text, lambda_handler


class TestPreprocessText:
    """Unit tests for the preprocess_text function"""
    
    def test_preprocess_text_basic(self):
        """Test basic text preprocessing with external stopwords"""
        # Mock the open function to simulate reading stopwords.txt
        mock_open = mock_builtins_open_with_content({'stopwords.txt': "a\nis\r\nthe\n"}).start()

        text = "This is a great product! I love it."
        result = preprocess_text(text)
        
        mock_open.stop() # Stop mocking after the function call

        assert isinstance(result, dict)
        assert 'original_text' in result
        assert 'tokens' in result
        assert 'processed_text' in result
        assert 'word_count' in result
        
        # Check that stopwords are removed (a, is, the) and punctuation
        assert 'is' not in result['tokens']
        assert 'a' not in result['tokens']
        assert 'the' not in result['tokens']
        assert '!' not in result['original_text'] # Punctuation removed from original_text before tokenization
        
        # Check that meaningful words are kept
        assert 'great' in result['tokens']
        assert 'product' in result['tokens']
        assert 'love' in result['tokens']
        
        # Check word count
        assert result['word_count'] == 4 # great, product, love, this

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
        """Test preprocessing with special characters and full punctuation removal"""
        # Mock the open function to simulate reading stopwords.txt
        mock_open = mock_builtins_open_with_content({'stopwords.txt': ""}).start() # Empty stopwords for this test

        text = "Amazing product!!! Best purchase ever. 5/5 stars ⭐⭐⭐⭐⭐"
        result = preprocess_text(text)
        
        mock_open.stop() # Stop mocking after the function call

        # Special characters and punctuation should be removed entirely
        assert '!!!' not in result['original_text'] # Punctuation removed
        assert '⭐' not in result['processed_text'] # Emojis (non-alphanumeric) removed
        assert result['original_text'] == 'amazing product best purchase ever 55 stars ' # Note space at end due to removal
        
        # Meaningful words should remain, numbers too
        assert 'amazing' in result['tokens']
        assert 'product' in result['tokens']
        assert 'best' in result['tokens']
        assert 'purchase' in result['tokens']
        assert 'ever' in result['tokens']
        assert '55' in result['tokens'] # Numbers are kept
        assert 'stars' in result['tokens']
        assert result['word_count'] == 7
    
    def test_preprocess_text_stemming(self):
        """Test that simple stemming works correctly"""
        # Mock the open function to simulate reading stopwords.txt
        mock_open = mock_builtins_open_with_content({'stopwords.txt': ""}).start()

        text = "running jumped happy quickly" # 'running', 'jumped', 'happy', 'quickly'
        result = preprocess_text(text)
        
        mock_open.stop() # Stop mocking after the function call

        tokens = result['tokens']
        processed_text = result['processed_text']
        
        # Check that simple stemming occurred
        assert 'run' in tokens
        assert 'jump' in tokens
        assert 'happi' in tokens # Simple stemming for 'happy' might result in this
        assert 'quick' in tokens
        
        assert 'running' not in tokens
        assert 'jumped' not in tokens
        assert 'quickly' not in tokens

    def test_preprocess_text_case_insensitive(self):
        """Test that preprocessing handles different cases"""
        # Mock the open function to simulate reading stopwords.txt
        mock_open = mock_builtins_open_open_with_content({'stopwords.txt': ""}).start()

        text = "EXCELLENT Product with GREAT Quality"
        result = preprocess_text(text)
        
        mock_open.stop()

        # Original text should be lowercased before full processing
        assert result['original_text'] == 'excellent product with great quality'
        
        # Tokens should contain lowercase words
        assert 'excellent' in result['tokens']
        assert 'product' in result['tokens']
        assert 'great' in result['tokens']
        assert 'quality' in result['tokens']


# Helper to mock builtins.open for reading external files
def mock_builtins_open_with_content(file_contents):
    """
    Returns a mock for builtins.open that provides specified content for given filenames.
    `file_contents` is a dict where keys are filenames and values are their content strings.
    """
    def mock_open(filename, mode='r', encoding=None):
        if 'r' in mode:
            if filename in file_contents:
                mock_file = MagicMock()
                mock_file.__enter__.return_value = mock_file
                mock_file.read.return_value = file_contents[filename]
                mock_file.__iter__.return_value = iter(file_contents[filename].splitlines())
                return mock_file
            else:
                # Raise FileNotFoundError if the file is not in our mocked content
                raise FileNotFoundError(f"No such file or directory: '{filename}' (mocked)")
        return MagicMock() # For 'w', 'a' modes, return a generic mock

    return patch('builtins.open', side_effect=mock_open)


class TestLambdaHandler:
    """Unit tests for the lambda_handler function"""
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_success(self, mock_s3_client):
        """Test successful lambda execution with EventBridge S3 event"""
        # Mock the open function for stopwords.txt
        mock_open = mock_builtins_open_with_content({'stopwords.txt': "a\nis\r\nthe\n"}).start()

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
        mock_s3_client.put_object.return_value = {} # Mock successful put_object

        # Create EventBridge S3 event
        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            'detail': {
                'bucket': {'name': 'test-raw-bucket'},
                'object': {'key': 'test-review.json'}
            }
        }
        
        # Set environment variable
        with patch.dict(os.environ, {'PROCESSED_BUCKET': 'test-processed-bucket'}):
            result = lambda_handler(event, {})
        
        # Verify response
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == '1 review(s) successfully preprocessed and sent to test-processed-bucket'
        assert body['processed_count'] == 1
        
        # Verify S3 calls
        mock_s3_client.get_object.assert_called_once_with(Bucket='test-raw-bucket', Key='test-review.json')
        mock_s3_client.put_object.assert_called_once()
        
        # Check put_object call arguments
        put_call_args = mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'test-processed-bucket'
        # The key is now review_id.json, check that it contains the review ID
        assert 'processed/B001234567.json' == put_call_args[1]['Key'] 
        assert put_call_args[1]['ContentType'] == 'application/json'

        mock_open.stop() # Stop mocking after the function call
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_missing_fields(self, mock_s3_client):
        """Test lambda execution with missing review fields"""
        mock_open = mock_builtins_open_with_content({'stopwords.txt': ""}).start()
        
        mock_review_data = {
            'asin': 'B001234567' # Missing other fields
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_review_data).encode('utf-8')))
        }
        mock_s3_client.put_object.return_value = {}
        
        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            'detail': {
                'bucket': {'name': 'test-raw-bucket'},
                'object': {'key': 'test-review.json'}
            }
        }
        
        with patch.dict(os.environ, {'PROCESSED_BUCKET': 'test-processed-bucket'}):
            result = lambda_handler(event, {})
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['processed_count'] == 1
        
        # Check the content of the put_object call for default values
        put_call_args = mock_s3_client.put_object.call_args
        sent_data = json.loads(put_call_args[1]['Body'])
        assert sent_data['reviewer_id'] == 'unknown'
        assert sent_data['total_word_count'] == 0

        mock_open.stop()
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_s3_error(self, mock_s3_client):
        """Test lambda execution with S3 error during get_object"""
        mock_open = mock_builtins_open_with_content({'stopwords.txt': ""}).start()

        mock_s3_client.get_object.side_effect = Exception("S3 GetObject Error")
        
        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            'detail': {
                'bucket': {'name': 'test-raw-bucket'},
                'object': {'key': 'test-review.json'}
            }
        }
        
        result = lambda_handler(event, {})
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to preprocess reviews'
        assert 'S3 GetObject Error' in body['details']

        mock_open.stop()
    
    @patch('lambda_function.s3_client')
    def test_lambda_handler_jsonl_format_multiple_reviews(self, mock_s3_client):
        """Test lambda execution with JSONL format containing multiple reviews"""
        mock_open = mock_builtins_open_with_content({'stopwords.txt': ""}).start()

        mock_review_data1 = {
            'asin': 'B001', 'reviewerID': 'A1', 'summary': 'Review 1', 'reviewText': 'Text 1'
        }
        mock_review_data2 = {
            'asin': 'B002', 'reviewerID': 'A2', 'summary': 'Review 2', 'reviewText': 'Text 2'
        }
        jsonl_content = f"{json.dumps(mock_review_data1)}\n{json.dumps(mock_review_data2)}\n"
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=jsonl_content.encode('utf-8')))
        }
        mock_s3_client.put_object.return_value = {} # Mock successful put_object
        
        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            'detail': {
                'bucket': {'name': 'test-raw-bucket'},
                'object': {'key': 'test-reviews.jsonl'}
            }
        }
        
        with patch.dict(os.environ, {'PROCESSED_BUCKET': 'test-processed-bucket'}):
            result = lambda_handler(event, {})
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == '2 review(s) successfully preprocessed and sent to test-processed-bucket'
        assert body['processed_count'] == 2
        
        # Verify put_object was called twice for each review
        assert mock_s3_client.put_object.call_count == 2
        
        # Check the calls were for different keys based on asin
        put_calls = mock_s3_client.put_object.call_args_list
        called_keys = {call.kwargs['Key'] for call in put_calls}
        assert 'processed/B001.json' in called_keys
        assert 'processed/B002.json' in called_keys

        mock_open.stop()

    @patch('lambda_function.s3_client')
    def test_lambda_handler_invalid_json_content(self, mock_s3_client):
        """Test lambda execution with invalid JSON content"""
        mock_open = mock_builtins_open_with_content({'stopwords.txt': ""}).start()

        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=b'invalid json'))
        }
        
        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            'detail': {
                'bucket': {'name': 'test-raw-bucket'},
                'object': {'key': 'invalid.json'}
            }
        }
        
        result = lambda_handler(event, {})
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to preprocess reviews'
        assert 'JSONDecodeError' in body['details'] # The error is now JSONDecodeError for invalid JSON

        mock_open.stop()

    @patch('lambda_function.s3_client')
    def test_lambda_handler_empty_detail(self, mock_s3_client):
        """Test lambda execution with an event missing the 'detail' key"""
        mock_open = mock_builtins_open_with_content({'stopwords.txt': ""}).start()

        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            # Missing 'detail' key
        }
        
        result = lambda_handler(event, {})
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to preprocess reviews'
        assert 'ValueError: Event does not contain \'detail\' key for S3 event.' in body['details']
        
        mock_open.stop()


if __name__ == '__main__':
    pytest.main([__file__])
