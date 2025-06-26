import pytest
import json
from unittest.mock import Mock, patch, MagicMock
import sys
import os

from moto.aws.mock_aws import mock_aws 


# Add the lambda function directory to the path
# Assuming this test file is in 'tests/' and lambda functions are in 'src/'
sys.path.append(os.path.join(os.path.dirname(__file__), '../lambda_functions/sentiment_analysis'))

# Import lambda_function after sys.path update
# We import it here for type hinting and initial module structure, but reload it in tests.
import lambda_function as sentiment_lambda_module_import # Renamed to avoid confusion with reloaded version

# Correct import for moto decorators
# Helper to mock builtins.open for reading external files (not strictly needed for sentiment_analysis but good for consistency)
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
                raise FileNotFoundError(f"No such file or directory: '{filename}' (mocked)")
        return MagicMock()

    return patch('builtins.open', side_effect=mock_open)


class TestAnalyzeSentimentInText:
    """Unit tests for the analyze_sentiment_in_text function"""

    def test_positive_sentiment(self):
        """Test text with positive sentiment"""
        text = "This is a great product with excellent quality. I love it!"
        result = sentiment_lambda_module_import.analyze_sentiment_in_text(text)
        
        assert isinstance(result, dict)
        assert result['pos'] > 0
        assert result['neg'] == 0
        assert result['compound'] > 0
        
        # Words in "This is a great product with excellent quality. I love it!"
        # words: ['this', 'is', 'a', 'great', 'product', 'with', 'excellent', 'quality', 'i', 'love', 'it'] (11 words)
        # POSITIVE_WORDS: {'great', 'excellent', 'love'} (3 words)
        # NEGATIVE_WORDS: {} (0 words)
        # total_words in function is 11
        # pos_score = 3/11 = 0.2727...
        # compound = (3-0)/11 = 0.2727...
        assert result['pos'] == round(3/11, 3) # Corrected calculation based on exact function logic
        assert result['compound'] == round(3/11, 3)


    def test_negative_sentiment(self):
        """Test text with negative sentiment"""
        text = "This product is bad and terrible. I hate it, it's awful!"
        result = sentiment_lambda_module_import.analyze_sentiment_in_text(text)
        
        assert result['neg'] > 0
        assert result['pos'] == 0
        assert result['compound'] < 0
        # words: ['this', 'product', 'is', 'bad', 'and', 'terrible', 'i', 'hate', 'it', 'it', 's', 'awful'] (12 words)
        # POSITIVE_WORDS: {}
        # NEGATIVE_WORDS: {'bad', 'terrible', 'hate', 'awful'} (4 words)
        # total_words in function is 12
        # neg_score = 4/12 = 0.3333...
        # compound = (0-4)/12 = -0.3333...
        assert result['neg'] == round(4/12, 3)
        assert result['compound'] == round(-4/12, 3)

    def test_neutral_sentiment(self):
        """Test text with neutral sentiment"""
        text = "This is a product. It has features."
        result = sentiment_lambda_module_import.analyze_sentiment_in_text(text)
        
        assert result['pos'] == 0
        assert result['neg'] == 0
        assert result['neu'] == 1 # All neutral
        assert result['compound'] == 0

    def test_empty_text(self):
        """Test empty text input"""
        result = sentiment_lambda_module_import.analyze_sentiment_in_text("")
        
        assert result['pos'] == 0
        assert result['neg'] == 0
        assert result['neu'] == 0
        assert result['compound'] == 0

    def test_none_input(self):
        """Test None input"""
        result = sentiment_lambda_module_import.analyze_sentiment_in_text(None)
        
        assert result['pos'] == 0
        assert result['neg'] == 0
        assert result['neu'] == 0
        assert result['compound'] == 0
    
    def test_mixed_sentiment(self):
        """Test text with mixed positive and negative words"""
        text = "This product is good but also bad."
        result = sentiment_lambda_module_import.analyze_sentiment_in_text(text)
        
        assert result['pos'] > 0
        assert result['neg'] > 0
        # Words: ['this', 'product', 'is', 'good', 'but', 'also', 'bad'] (7 words)
        # 'good' (1), 'bad' (1).
        # Compound: (1-1)/7 = 0
        assert result['compound'] == 0

# Helper to create an EventBridge S3 event (copied from test_integration.py)
import uuid
def create_eventbridge_s3_event(bucket_name, object_key):
    return {
        'version': '0',
        'id': str(uuid.uuid4()),
        'detail-type': 'Object Created',
        'source': 'aws.s3',
        'account': '000000000000',
        'time': '2025-06-24T12:00:00Z',
        'region': 'us-east-1',
        'resources': [f'arn:aws:s3:::{bucket_name}'],
        'detail': {
            'version': '0',
            'bucket': {'name': bucket_name},
            'object': {'key': object_key, 'size': 100, 'etag': 'mock-etag', 'sequencer': 'mock-sequencer'},
            'request-id': str(uuid.uuid4()),
            'requester': 'mock-requester',
            'source-ip-address': '127.0.0.1',
            'reason': 'PutObject'
        },
        'event-bus-name': 'default'
    }


# Apply mock_aws decorator at the class level
@mock_aws(services=['s3', 'ssm']) 
class TestLambdaHandler:
    """Unit tests for the lambda_handler function"""
    
    # Helper method to reload the lambda_function module and re-patch globals for each test
    def reload_lambda_module(self, mock_ssm_error=False):
        # Remove the module from sys.modules to force a fresh import
        if 'lambda_function' in sys.modules:
            del sys.modules['lambda_function']
        
        # Temporarily mock get_parameter during the reload if an SSM error is needed
        mock_get_parameter_patch = None
        if mock_ssm_error:
            # Patch get_parameter to raise an exception during the module's cold start
            mock_get_parameter_patch = patch('lambda_function.get_parameter', side_effect=Exception("Simulated SSM Error for Reload"))
            mock_get_parameter_patch.start()
        
        # Re-import to re-initialize global variables/clients under the active moto mocks
        # This is where the actual lambda_function module is reloaded, ensuring global clients
        # are initialized within moto's mocked environment.
        import lambda_function as reloaded_sentiment_lambda_module
        # Update the reference to the lambda module for the test class to use
        self.sentiment_lambda = reloaded_sentiment_lambda_module

        if mock_get_parameter_patch:
            mock_get_parameter_patch.stop() # Stop the patch after reload

    # Setup method to run before each test
    def setup_method(self, method):
        # Ensure the lambda module is reloaded and clients are mocked for each test
        self.reload_lambda_module()


    def test_lambda_handler_positive_review(self):
        """Test lambda execution with positive review (EventBridge S3 event)"""
        # SSM and S3 clients are implicitly mocked by @mock_aws
        ssm_client = boto3.client('ssm', region_name='us-east-1')
        ssm_client.put_parameter(Name='/my-app/s3/final_reviews_bucket_name', Type='String', Value='mock-final-reviews-bucket', Overwrite=True)

        input_bucket = 'test-clean-bucket'
        final_reviews_bucket = 'mock-final-reviews-bucket'
        
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=input_bucket)
        s3_client.create_bucket(Bucket=final_reviews_bucket)

        mock_review_data = {
            'review_id': 'B001234567',
            'reviewer_id': 'A1234567890',
            'overall_rating': 5,
            'processed_summary': 'great product',
            'processed_reviewText': 'excellent quality recommend',
            'processed_overall': '5',
            'processing_stage': 'profanity_checked',
            'summary_word_count': 2, # Example word counts for weighted average
            'reviewText_word_count': 3,
            'overall_word_count': 1,
            'total_word_count': 6
        }
        
        input_key = 'clean/B001234567.json'
        s3_client.put_object(
            Bucket=input_bucket,
            Key=input_key,
            Body=json.dumps(mock_review_data),
            ContentType='application/json'
        )
        
        event = create_eventbridge_s3_event(input_bucket, input_key)
        
        result = self.sentiment_lambda.lambda_handler(event, {}) # Call the reloaded lambda_handler
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Sentiment analysis completed'
        assert body['sentiment_label'] == 'positive'
        assert 'mock-final-reviews-bucket' in body['output_location']
        
        s3_client.get_object.assert_called_once_with(Bucket='test-clean-bucket', Key='clean/B001234567.json')
        put_call_args = s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'mock-final-reviews-bucket'
        assert 'analyzed/B001234567.json' == put_call_args[1]['Key']

        sent_data = json.loads(put_call_args[1]['Body'])
        assert sent_data['processing_stage'] == 'sentiment_analyzed'
        assert 'sentiment_analysis' in sent_data
        assert sent_data['sentiment_analysis']['sentiment_label'] == 'positive'
        assert sent_data['sentiment_analysis']['aggregated_compound_score'] > 0


    def test_lambda_handler_negative_review(self):
        """Test lambda execution with negative review (EventBridge S3 event)"""
        # self.reload_lambda_module() # Already called in setup_method

        ssm_client = boto3.client('ssm', region_name='us-east-1')
        ssm_client.put_parameter(Name='/my-app/s3/final_reviews_bucket_name', Type='String', Value='mock-final-reviews-bucket', Overwrite=True)

        input_bucket = 'test-flagged-bucket'
        final_reviews_bucket = 'mock-final-reviews-bucket'
        
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=input_bucket)
        s3_client.create_bucket(Bucket=final_reviews_bucket)

        mock_review_data = {
            'review_id': 'B001234568',
            'reviewer_id': 'A1234567891',
            'overall_rating': 1,
            'processed_summary': 'bad product',
            'processed_reviewText': 'terrible quality hate',
            'processed_overall': '1',
            'processing_stage': 'profanity_checked',
            'summary_word_count': 2,
            'reviewText_word_count': 3,
            'overall_word_count': 1,
            'total_word_count': 6
        }
        
        input_key = 'flagged/B001234568.json'
        s3_client.put_object(
            Bucket=input_bucket,
            Key=input_key,
            Body=json.dumps(mock_review_data),
            ContentType='application/json'
        )
        
        event = create_eventbridge_s3_event(input_bucket, input_key)
        
        result = self.sentiment_lambda.lambda_handler(event, {}) # Call the reloaded lambda_handler
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Sentiment analysis completed'
        assert body['sentiment_label'] == 'negative'
        
        s3_client.get_object.assert_called_once_with(Bucket='test-flagged-bucket', Key='flagged/B001234568.json')
        put_call_args = s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'mock-final-reviews-bucket'
        assert 'analyzed/B001234568.json' == put_call_args[1]['Key']

        sent_data = json.loads(put_call_args[1]['Body'])
        assert sent_data['sentiment_analysis']['sentiment_label'] == 'negative'
        assert sent_data['sentiment_analysis']['aggregated_compound_score'] < 0

    def test_lambda_handler_s3_error(self):
        """Test lambda execution with S3 error during get_object"""
        # self.reload_lambda_module() # Already called in setup_method

        ssm_client = boto3.client('ssm', region_name='us-east-1')
        ssm_client.put_parameter(Name='/my-app/s3/final_reviews_bucket_name', Type='String', Value='mock-final-reviews-bucket', Overwrite=True)

        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.get_object.side_effect = Exception("S3 GetObject Error")
        
        event = create_eventbridge_s3_event('test-clean-bucket', 'clean/test-review.json')
        
        result = self.sentiment_lambda.lambda_handler(event, {})
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to perform sentiment analysis'
        assert 'S3 GetObject Error' in body['details']

    def test_lambda_handler_empty_detail(self):
        """Test lambda execution with an event missing the 'detail' key"""
        # self.reload_lambda_module() # Already called in setup_method
        
        ssm_client = boto3.client('ssm', region_name='us-east-1')
        ssm_client.put_parameter(Name='/my-app/s3/final_reviews_bucket_name', Type='String', Value='mock-final-reviews-bucket', Overwrite=True)

        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            # Missing 'detail' key
        }
        
        result = self.sentiment_lambda.lambda_handler(event, {})
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to perform sentiment analysis'
        assert 'ValueError: Event does not contain \'detail\' key for S3 event.' in body['details']

    def test_lambda_handler_ssm_error_fallback(self):
        """Test lambda execution when SSM parameters fail to load at cold start, should use fallback"""
        self.reload_lambda_module(mock_ssm_error=True) # Reload and trigger SSM error
        
        # We need to ensure a mock S3 client is available for put_object,
        # even if SSM parameter fetching for the bucket name fails globally.
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='clean-reviews-bucket') # Ensure input bucket exists
        s3_client.create_bucket(Bucket='final-reviews-bucket') # Ensure fallback output bucket exists

        mock_review_data = {
            'review_id': 'B001234567',
            'reviewer_id': 'A1234567890',
            'overall_rating': 5,
            'processed_summary': 'great product',
            'processed_reviewText': 'excellent quality recommend',
            'processed_overall': '5',
            'processing_stage': 'profanity_checked',
            'summary_word_count': 2,
            'reviewText_word_count': 3,
            'overall_word_count': 1,
            'total_word_count': 6
        }
        
        input_key = 'clean/B001234567.json'
        s3_client.put_object(
            Bucket='clean-reviews-bucket', # Default input bucket
            Key=input_key,
            Body=json.dumps(mock_review_data),
            ContentType='application/json'
        )
        
        event = create_eventbridge_s3_event('clean-reviews-bucket', input_key)
        
        result = self.sentiment_lambda.lambda_handler(event, {})
        
        assert result['statusCode'] == 200 # Should still succeed, using fallback buckets
        body = json.loads(result['body'])
        assert body['message'] == 'Sentiment analysis completed'
        assert body['sentiment_label'] == 'positive'
        assert 'final-reviews-bucket' in body['output_location'] # Should use the fallback bucket

        put_call_args = s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'final-reviews-bucket'
        assert 'analyzed/B001234567.json' == put_call_args[1]['Key']


# Import uuid for unique IDs
import uuid

if __name__ == '__main__':
    pytest.main([__file__])
