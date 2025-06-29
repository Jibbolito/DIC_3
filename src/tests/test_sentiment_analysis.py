import pytest
import json
import os
import sys
from unittest.mock import Mock, patch, create_autospec
import importlib
import logging
import nltk

logger = logging.getLogger()
logger.setLevel(logging.INFO)


lambda_function = None

def setup_module(module):
    """
    Called once when the test module is imported.
    Downloads NLTK vader_lexicon if not already present, and ensures lambda_function is correctly loaded.
    """
    global lambda_function

    
    lambda_func_dir = os.path.join(os.path.dirname(__file__), '../lambda_functions/sentiment_analysis')
    if lambda_func_dir not in sys.path:
        sys.path.insert(0, lambda_func_dir)

    
    nltk_data_path = os.path.join(lambda_func_dir, 'nltk_data')
    if nltk_data_path not in nltk.data.path:
        nltk.data.path.append(nltk_data_path)

    try:
        nltk.data.find('sentiment/vader_lexicon.zip')
    except LookupError:
        print("Downloading NLTK vader_lexicon...")
        nltk.download('vader_lexicon', quiet=True)
    
    
    
    if 'lambda_function' in sys.modules:
        del sys.modules['lambda_function']
    
    
    importlib.reload(nltk)
    
    
    
    import lambda_function as lf_module
    lambda_function = lf_module

    
    
    lambda_function.analyzer = nltk.sentiment.vader.SentimentIntensityAnalyzer()


class TestSentimentAnalysisLambda:

    
    
    @patch('lambda_function.s3_client')
    @patch('lambda_function.ssm_client')
    @patch('lambda_function.dynamodb')
    def test_get_parameter_success(self, mock_dynamodb, mock_ssm_client, mock_s3_client):
        """Test successful retrieval of an SSM parameter."""
        mock_ssm_client.get_parameter.return_value = {
            'Parameter': {'Value': 'test-final-bucket'}
        }
        
        parameter_value = lambda_function.get_parameter('/my-app/s3/final_reviews_bucket_name')
        assert parameter_value == 'test-final-bucket'

    @patch('lambda_function.s3_client')
    @patch('lambda_function.ssm_client')
    @patch('lambda_function.dynamodb')
    def test_get_parameter_failure(self, mock_dynamodb, mock_ssm_client, mock_s3_client):
        """Test failure in retrieving an SSM parameter."""
        mock_ssm_client.get_parameter.side_effect = Exception("ParameterNotFound")
        
        with pytest.raises(Exception) as excinfo:
            lambda_function.get_parameter('/non-existent-parameter')
        assert "ParameterNotFound" in str(excinfo.value)
    
    @patch('lambda_function.analyze_sentiment_in_text') 
    def test_analyze_sentiment_in_text(self, mock_analyze_sentiment_in_text):
        """Test sentiment analysis logic."""
        
        mock_analyze_sentiment_in_text.side_effect = [
            {'compound': 0.8, 'pos': 0.5, 'neg': 0.1, 'neu': 0.4, 'label': 'positive'},
            {'compound': -0.8, 'pos': 0.1, 'neg': 0.5, 'neu': 0.4, 'label': 'negative'},
            {'compound': 0.0, 'pos': 0.3, 'neg': 0.3, 'neu': 0.4, 'label': 'neutral'},
            {'compound': 0.0, 'pos': 0.3, 'neg': 0.3, 'neu': 0.4, 'label': 'neutral'}
        ]

        positive_text = "This product is fantastic and I love it!"
        negative_text = "This is a terrible and awful experience."
        neutral_text = "The product is ok, nothing special."
        mixed_text = "The service was great, but the food was awful."

        
        pos_result = lambda_function.analyze_sentiment_in_text(positive_text)
        neg_result = lambda_function.analyze_sentiment_in_text(negative_text)
        neu_result = lambda_function.analyze_sentiment_in_text(neutral_text)
        mixed_result = lambda_function.analyze_sentiment_in_text(mixed_text)

        assert pos_result['compound'] > 0.5
        assert pos_result['label'] == 'positive'

        assert neg_result['compound'] < -0.5
        assert neg_result['label'] == 'negative'

        assert -0.5 <= neu_result['compound'] <= 0.5
        assert neu_result['label'] == 'neutral'

        assert -0.5 <= mixed_result['compound'] <= 0.5
        assert mixed_result['label'] == 'neutral'

    @patch('lambda_function.s3_client')
    @patch('lambda_function.ssm_client')
    @patch('lambda_function.dynamodb')
    
    @patch('lambda_function.FINAL_REVIEWS_BUCKET', 'test-final-reviews-bucket')
    def test_lambda_handler_success(self, mock_dynamodb, mock_ssm_client, mock_s3_client):
        """Test successful execution of the lambda handler."""
        
        mock_ssm_client.get_parameter.side_effect = [
            {'Parameter': {'Value': 'test-final-reviews-bucket'}}
        ]

        mock_table = Mock()
        mock_dynamodb.Table.return_value = mock_table
        
        mock_review_data = {
            'review_id': 'test_review_123',
            'reviewer_id': 'reviewer_456',
            'overall_rating': 5,
            'processed_summary': 'This is an absolutely amazing product!',
            'processed_reviewText': 'Highly recommended. I love it.',
            'processed_overall': 'positive review',
            'summary_word_count': 6,
            'reviewText_word_count': 6,
            'overall_word_count': 2,
            'total_word_count': 14,
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
                        'bucket': {'name': 'processed-reviews-bucket'},
                        'object': {'key': 'profanity-checked/test-review.json'}
                    }
                }
            ]
        }
        
        
        with patch('lambda_function._config_loaded', False, create=True):
            result = lambda_function.lambda_handler(event, {}) 
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Sentiment analysis completed'
        assert body['review_id'] == 'test_review_123'
        assert body['sentiment_label'] == 'positive'
        
        assert 'test-final-reviews-bucket' in body['output_location']
        
        mock_s3_client.get_object.assert_called_once()
        mock_s3_client.put_object.assert_called_once()
        
        put_call_args = mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'test-final-reviews-bucket'
        assert 'analyzed/' in put_call_args[1]['Key']
        
        
        
        saved_data = json.loads(put_call_args[1]['Body'])
        assert saved_data['review_id'] == 'test_review_123'
        assert saved_data['sentiment_analysis']['sentiment_label'] == 'positive'

    @patch('lambda_function.s3_client')
    @patch('lambda_function.ssm_client')
    @patch('lambda_function.dynamodb')
    def test_lambda_handler_s3_get_object_error(self, mock_dynamodb, mock_ssm_client, mock_s3_client):
        """Test lambda handler's error handling for S3 GetObject errors."""
        mock_ssm_client.get_parameter.return_value = {
            'Parameter': {'Value': 'test-final-reviews-bucket'}
        }
        
        mock_s3_client.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})
        mock_s3_client.get_object.side_effect = mock_s3_client.exceptions.NoSuchKey("No such key exists")

        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'processed-reviews-bucket'},
                        'object': {'key': 'profanity-checked/non_existent.json'}
                    }
                }
            ]
        }
        
        with patch('lambda_function._config_loaded', False, create=True):
            result = lambda_function.lambda_handler(event, {}) 
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert 'Failed to perform sentiment analysis' in body['error']
        assert 'No such key exists' in body['details']

    @patch('lambda_function.s3_client')
    @patch('lambda_function.ssm_client')
    @patch('lambda_function.dynamodb')
    def test_lambda_handler_json_decode_error(self, mock_dynamodb, mock_ssm_client, mock_s3_client):
        """Test lambda handler's error handling for invalid JSON."""
        mock_ssm_client.get_parameter.return_value = {
            'Parameter': {'Value': 'test-final-reviews-bucket'}
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=b'{"invalid_json": "true}}')) 
        }
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': 'processed-reviews-bucket'},
                        'object': {'key': 'profanity-checked/malformed.json'}
                    }
                }
            ]
        }
        
        with patch('lambda_function._config_loaded', False, create=True):
            result = lambda_function.lambda_handler(event, {}) 
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert 'Failed to perform sentiment analysis' in body['error']
        assert 'Unterminated string' in body['details']

    @patch('lambda_function.s3_client')
    @patch('lambda_function.ssm_client')
    @patch('lambda_function.dynamodb')
    def test_lambda_handler_ssm_parameter_error_and_fallback(self, mock_dynamodb, mock_ssm_client, mock_s3_client):
        """
        Test lambda handler's behavior when SSM parameter retrieval fails,
        ensuring it falls back to the default bucket name.
        """
        mock_ssm_client.get_parameter.side_effect = Exception("Simulated SSM error during cold start")

        mock_table = Mock()
        mock_dynamodb.Table.return_value = mock_table;

        mock_review_data = {
            'review_id': 'review_with_fallback',
            'reviewer_id': 'reviewer_xyz',
            'processed_summary': 'Good',
            'processed_reviewText': 'This is good.',
            'processed_overall': 'okay',
            'summary_word_count': 1,
            'reviewText_word_count': 3,
            'overall_word_count': 1,
            'total_word_count': 5,
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
                        'bucket': {'name': 'processed-reviews-bucket'},
                        'object': {'key': 'profanity-checked/review_with_fallback.json'}
                    }
                }
            ]
        }
        
        with patch('lambda_function._config_loaded', False, create=True):
            result = lambda_function.lambda_handler(event, {}) 
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert 'final-reviews-bucket' in body['output_location']
        
        put_call_args = mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'final-reviews-bucket'
        assert 'analyzed/' in put_call_args[1]['Key']


if __name__ == '__main__':
    pytest.main([__file__])