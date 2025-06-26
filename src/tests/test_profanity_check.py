import pytest
import json
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the lambda function directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../lambda_functions/profanity_check'))

from lambda_function import check_profanity_in_text, lambda_handler

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


class TestCheckProfanityInText:
    """Unit tests for the check_profanity_in_text function"""
    
    @patch('lambda_function.ProfanityFilter')
    def test_clean_text(self, MockProfanityFilter):
        """Test text without profanity"""
        # Mock ProfanityFilter to return non-profane for clean text
        mock_pf_instance = MockProfanityFilter.return_value
        mock_pf_instance.is_profane.return_value = False
        mock_pf_instance.censor.side_effect = lambda x: x # No censoring

        text = "This is a great product with excellent quality"
        result = check_profanity_in_text(text)
        
        assert isinstance(result, dict)
        assert result['contains_profanity'] is False
        assert result['profanity_words'] == []
        assert result['profanity_count'] == 0
        assert result['severity_score'] == 0

    @patch('lambda_function.ProfanityFilter')
    def test_profanity_detection(self, MockProfanityFilter):
        """Test text with profanity"""
        # Mock ProfanityFilter to return profane and censor text
        mock_pf_instance = MockProfanityFilter.return_value
        mock_pf_instance.is_profane.return_value = True
        mock_pf_instance.censor.side_effect = lambda x: x.replace('crappy', '******').replace('damn', '****')

        text = "This product is crappy and the service is damn awful"
        result = check_profanity_in_text(text)
        
        assert result['contains_profanity'] is True
        assert result['severity_score'] > 0
        assert 'censored_text' in result
        
        # Check that censored text is different from original
        assert result['censored_text'] != text
        assert '******' in result['censored_text']
        assert '****' in result['censored_text']
        
        # profanityfilter should detect and censor the text
        assert len(result['censored_text']) > 0
        assert 'crappy' in result['profanity_words']
        assert 'damn' in result['profanity_words']
    
    @patch('lambda_function.ProfanityFilter')
    def test_empty_text(self, MockProfanityFilter):
        """Test empty text input"""
        result = check_profanity_in_text("")
        
        assert result['contains_profanity'] is False
        assert result['profanity_words'] == []
        assert result['profanity_count'] == 0
        assert result['severity_score'] == 0
        assert result['censored_text'] == ''
    
    @patch('lambda_function.ProfanityFilter')
    def test_none_input(self, MockProfanityFilter):
        """Test None input"""
        result = check_profanity_in_text(None)
        
        assert result['contains_profanity'] is False
        assert result['profanity_words'] == []
        assert result['profanity_count'] == 0
        assert result['severity_score'] == 0
        assert result['censored_text'] == '' # Should also return empty string for censored_text

    @patch('lambda_function.ProfanityFilter')
    def test_case_insensitive_detection(self, MockProfanityFilter):
        """Test that profanity detection is case insensitive"""
        mock_pf_instance = MockProfanityFilter.return_value
        mock_pf_instance.is_profane.return_value = True
        mock_pf_instance.censor.side_effect = lambda x: x.replace('SHIT', '****').replace('Damn', '****')

        text = "This product is SHIT and the service is Damn awful"
        result = check_profanity_in_text(text)
        
        assert result['contains_profanity'] is True
        assert result['profanity_count'] > 0
        assert 'shit' in result['profanity_words']
        assert 'damn' in result['profanity_words']

    @patch('lambda_function.ProfanityFilter')
    def test_punctuation_handling(self, MockProfanityFilter):
        """Test profanity detection with punctuation"""
        mock_pf_instance = MockProfanityFilter.return_value
        mock_pf_instance.is_profane.return_value = True
        mock_pf_instance.censor.side_effect = lambda x: x.replace('shit', '****').replace('damn', '****')

        text = "This is shit! The service is damn, awful."
        result = check_profanity_in_text(text)
        
        assert result['contains_profanity'] is True
        profanity_words = result['profanity_words']
        assert 'shit' in profanity_words
        assert 'damn' in profanity_words
    
    @patch('lambda_function.ProfanityFilter')
    def test_severity_scoring(self, MockProfanityFilter):
        """Test severity scoring system with base and high-severity words"""
        mock_pf_instance = MockProfanityFilter.return_value
        mock_pf_instance.is_profane.side_effect = [True, True] # For two calls
        mock_pf_instance.censor.side_effect = [
            lambda x: x.replace('bad', '***').replace('damn', '****'),
            lambda x: x.replace('fucking', '*******').replace('shit', '****')
        ]
        
        mild_text = "This is damn bad"
        severe_text = "This is fucking shit"
        
        # Need to re-patch open for each test if PfFilter is initialized multiple times
        with mock_builtins_open_with_content({'custom_profanity.txt': ""}).start():
            mild_result = check_profanity_in_text(mild_text)
            severe_result = check_profanity_in_text(severe_text)
        
        # Severe text should have higher severity score
        # 'damn' (3) + 'bad' (2) = 5 for mild
        # 'fucking' (3) + 'shit' (3) = 6 for severe
        assert mild_result['severity_score'] == (1*2 + 3) # One profane word + high severity
        assert severe_result['severity_score'] == (2*2 + 3 + 3) # Two profane words + two high severity
        assert severe_result['severity_score'] > mild_result['severity_score']
    
    @patch('lambda_function.ProfanityFilter')
    def test_multiple_occurrences(self, MockProfanityFilter):
        """Test detection of multiple profanity words and unique counting"""
        mock_pf_instance = MockProfanityFilter.return_value
        mock_pf_instance.is_profane.return_value = True
        mock_pf_instance.censor.side_effect = lambda x: x.replace('shit', '****').replace('damn', '****').replace('terrible', '********')

        text = "This shit is damn awful and fucking terrible, another shit"
        result = check_profanity_in_text(text)
        
        assert result['contains_profanity'] is True
        # Count of unique words
        assert result['profanity_count'] >= 3 # 'shit', 'damn', 'fucking', 'terrible'
        assert 'shit' in result['profanity_words']
        assert 'damn' in result['profanity_words']
        assert 'fucking' in result['profanity_words']
        assert 'terrible' in result['profanity_words']
        assert len(result['profanity_words']) >= 3 # Check for at least 3 unique words

    @patch('lambda_function.ProfanityFilter')
    def test_custom_profanity_words_loading(self, MockProfanityFilter):
        """Test that custom profanity words are loaded from file"""
        # Mock the open function to simulate reading custom_profanity.txt
        mock_open = mock_builtins_open_with_content({'custom_profanity.txt': "scam\nfraud\n"}).start()

        # Initialize profanity filter (this happens globally in lambda_function, so test setup needs to run this)
        # Re-import lambda_function to force re-initialization with mocked open
        with patch.dict(sys.modules, {'lambda_function': sys.modules['lambda_function']}):
            import lambda_function
            lambda_function.pf = ProfanityFilter(extra_censor_list=list(lambda_function.CUSTOM_PROFANITY_WORDS))
            
            text = "This is a total scam!"
            result = lambda_function.check_profanity_in_text(text)
            
            assert result['contains_profanity'] is True
            assert 'scam' in result['profanity_words']
        
        mock_open.stop()


class TestLambdaHandler:
    """Unit tests for the lambda_handler function"""
    
    @patch('lambda_function.s3_client')
    @patch('lambda_function.customer_profanity_table')
    @patch('lambda_function.get_parameter') # Mock SSM parameter store
    def test_lambda_handler_clean_review(self, mock_get_parameter, mock_customer_profanity_table, mock_s3_client):
        """Test lambda execution with clean review (EventBridge S3 event)"""
        # Mock SSM parameters
        mock_get_parameter.side_effect = lambda name: {
            '/my-app/s3/flagged_bucket_name': 'mock-flagged-bucket',
            '/my-app/s3/clean_bucket_name': 'mock-clean-bucket',
            '/my-app/dynamodb/customer_profanity_table_name': 'MockProfanityCounts',
            '/my-app/ban_threshold': '3'
        }.get(name)

        # Mock the open function for custom_profanity.txt
        mock_open = mock_builtins_open_with_content({'custom_profanity.txt': ""}).start()

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
        
        # Create EventBridge S3 event
        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            'detail': {
                'bucket': {'name': 'test-processed-bucket'},
                'object': {'key': 'processed/B001234567.json'}
            }
        }
        
        with patch.dict(os.environ, {
            'CLEAN_BUCKET': 'mock-clean-bucket',
            'FLAGGED_BUCKET': 'mock-flagged-bucket'
        }):
            result = lambda_handler(event, {})
        
        # Verify response
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Profanity check completed'
        assert body['contains_profanity'] is False
        assert body['profanity_count'] == 0
        assert body['severity_score'] == 0
        assert 'mock-clean-bucket' in body['output_location']
        
        # Verify S3 calls
        mock_s3_client.get_object.assert_called_once_with(Bucket='test-processed-bucket', Key='processed/B001234567.json')
        mock_s3_client.put_object.assert_called_once()
        
        # Check put_object call goes to clean bucket
        put_call_args = mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'mock-clean-bucket'
        assert 'clean/B001234567.json' == put_call_args[1]['Key']

        # Verify DynamoDB was NOT called for a clean review
        mock_customer_profanity_table.update_item.assert_not_called()

        mock_open.stop()
    
    @patch('lambda_function.s3_client')
    @patch('lambda_function.customer_profanity_table')
    @patch('lambda_function.get_parameter')
    def test_lambda_handler_profane_review(self, mock_get_parameter, mock_customer_profanity_table, mock_s3_client):
        """Test lambda execution with profane review (EventBridge S3 event)"""
        # Mock SSM parameters
        mock_get_parameter.side_effect = lambda name: {
            '/my-app/s3/flagged_bucket_name': 'mock-flagged-bucket',
            '/my-app/s3/clean_bucket_name': 'mock-clean-bucket',
            '/my-app/dynamodb/customer_profanity_table_name': 'MockProfanityCounts',
            '/my-app/ban_threshold': '3'
        }.get(name)

        # Mock the open function for custom_profanity.txt
        mock_open = mock_builtins_open_with_content({'custom_profanity.txt': "crappy\ndamn\n"}).start()

        mock_review_data = {
            'review_id': 'B001234567',
            'reviewer_id': 'A1234567890',
            'overall_rating': 1,
            'processed_summary': 'crappy product terrible quality',
            'processed_reviewText': 'awful product waste money damn',
            'processed_overall': '',
            'processing_stage': 'preprocessed'
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_review_data).encode('utf-8')))
        }
        mock_s3_client.put_object.return_value = {}

        # Mock DynamoDB update_item response for profanity count
        mock_customer_profanity_table.update_item.return_value = {
            'Attributes': {'profanity_count': {'N': '1'}} # First update, count becomes 1
        }
        
        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            'detail': {
                'bucket': {'name': 'test-processed-bucket'},
                'object': {'key': 'processed/B001234567.json'}
            }
        }
        
        with patch.dict(os.environ, {
            'CLEAN_BUCKET': 'mock-clean-bucket',
            'FLAGGED_BUCKET': 'mock-flagged-bucket'
        }):
            result = lambda_handler(event, {})
        
        # Verify response
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Profanity check completed'
        assert body['contains_profanity'] is True
        assert body['profanity_count'] > 0
        assert body['severity_score'] > 0
        assert 'mock-flagged-bucket' in body['output_location']
        assert body['reviewer_banned'] is False # Not banned yet, count is 1, threshold 3
        assert body['profanity_count'] >= 2 # 'crappy', 'damn'
        
        # Check put_object call goes to flagged bucket
        put_call_args = mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'mock-flagged-bucket'
        assert 'flagged/B001234567.json' == put_call_args[1]['Key']

        # Verify DynamoDB update_item was called
        mock_customer_profanity_table.update_item.assert_called_once()
        # Verify profanity count in the output
        sent_data = json.loads(put_call_args[1]['Body'])
        assert sent_data['profanity_analysis']['current_reviewer_profanity_count'] == 1 # Based on mock DDB response

        mock_open.stop()

    @patch('lambda_function.s3_client')
    @patch('lambda_function.customer_profanity_table')
    @patch('lambda_function.get_parameter')
    def test_lambda_handler_reviewer_banned(self, mock_get_parameter, mock_customer_profanity_table, mock_s3_client):
        """Test lambda execution with reviewer getting banned"""
        # Mock SSM parameters for a low ban threshold (e.g., 1)
        mock_get_parameter.side_effect = lambda name: {
            '/my-app/s3/flagged_bucket_name': 'mock-flagged-bucket',
            '/my-app/s3/clean_bucket_name': 'mock-clean-bucket',
            '/my-app/dynamodb/customer_profanity_table_name': 'MockProfanityCounts',
            '/my-app/ban_threshold': '1' # Set ban threshold to 1 for this test
        }.get(name)

        mock_open = mock_builtins_open_with_content({'custom_profanity.txt': "crappy\n"}).start()

        mock_review_data = {
            'review_id': 'B001234568',
            'reviewer_id': 'A1234567891',
            'overall_rating': 1,
            'processed_summary': 'crappy product',
            'processed_reviewText': 'this is bad',
            'processed_overall': '',
            'processing_stage': 'preprocessed'
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_review_data).encode('utf-8')))
        }
        mock_s3_client.put_object.return_value = {}

        # Mock DynamoDB update_item response for profanity count
        mock_customer_profanity_table.update_item.side_effect = [
            {'Attributes': {'profanity_count': {'N': '2'}}}, # First call: increment count to 2
            {'Attributes': {}} # Second call: for is_banned update, return values not needed for this test
        ]
        
        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            'detail': {
                'bucket': {'name': 'test-processed-bucket'},
                'object': {'key': 'processed/B001234568.json'}
            }
        }
        
        with patch.dict(os.environ, {
            'CLEAN_BUCKET': 'mock-clean-bucket',
            'FLAGGED_BUCKET': 'mock-flagged-bucket'
        }):
            result = lambda_handler(event, {})
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['contains_profanity'] is True
        assert body['reviewer_banned'] is True # Should be banned
        assert body['profanity_count'] > 0
        
        # Verify DynamoDB update_item was called twice (increment and ban)
        assert mock_customer_profanity_table.update_item.call_count == 2
        
        put_call_args = mock_s3_client.put_object.call_args
        sent_data = json.loads(put_call_args[1]['Body'])
        assert sent_data['profanity_analysis']['current_reviewer_profanity_count'] == 2 # Based on mock DDB response
        assert sent_data['profanity_analysis']['reviewer_banned'] is True

        mock_open.stop()

    @patch('lambda_function.s3_client')
    @patch('lambda_function.customer_profanity_table')
    @patch('lambda_function.get_parameter')
    def test_lambda_handler_s3_error(self, mock_get_parameter, mock_customer_profanity_table, mock_s3_client):
        """Test lambda execution with S3 error during get_object"""
        mock_get_parameter.side_effect = lambda name: { # Mock to prevent SSM init error
            '/my-app/s3/flagged_bucket_name': 'mock-flagged-bucket',
            '/my-app/s3/clean_bucket_name': 'mock-clean-bucket',
            '/my-app/dynamodb/customer_profanity_table_name': 'MockProfanityCounts',
            '/my-app/ban_threshold': '3'
        }.get(name)
        mock_open = mock_builtins_open_with_content({'custom_profanity.txt': ""}).start()

        mock_s3_client.get_object.side_effect = Exception("S3 GetObject Error")
        
        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            'detail': {
                'bucket': {'name': 'test-processed-bucket'},
                'object': {'key': 'processed/test-review.json'}
            }
        }
        
        result = lambda_handler(event, {})
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to perform profanity check'
        assert 'S3 GetObject Error' in body['details']

        mock_open.stop()

    @patch('lambda_function.s3_client')
    @patch('lambda_function.customer_profanity_table')
    @patch('lambda_function.get_parameter')
    def test_lambda_handler_ssm_error_fallback(self, mock_get_parameter, mock_customer_profanity_table, mock_s3_client):
        """Test lambda execution when SSM parameters fail to load at cold start"""
        # Simulate SSM parameter retrieval failure
        mock_get_parameter.side_effect = Exception("SSM Error during startup")

        mock_open = mock_builtins_open_with_content({'custom_profanity.txt': ""}).start()

        mock_review_data = {
            'review_id': 'B001234567',
            'reviewer_id': 'A1234567890',
            'processed_summary': 'crappy product',
            'processed_reviewText': 'bad service',
            'processed_overall': '',
            'processing_stage': 'preprocessed'
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_review_data).encode('utf-8')))
        }
        mock_s3_client.put_object.return_value = {}
        
        event = {
            'detail-type': 'Object Created',
            'source': 'aws.s3',
            'detail': {
                'bucket': {'name': 'test-processed-bucket'},
                'object': {'key': 'processed/B001234567.json'}
            }
        }
        
        # We need to temporarily remove the module from sys.modules to force re-initialization
        # so the mocked get_parameter is called during the 'cold start' part of the lambda
        if 'lambda_function' in sys.modules:
            del sys.modules['lambda_function']
        
        from lambda_function import lambda_handler as reloaded_lambda_handler # Reload
        
        # The environment variables are set to the *fallback* values from the lambda
        # due to SSM error, which is covered by the lambda's own try-except.
        # So here we don't need to patch os.environ directly for buckets, as the
        # lambda itself will use the fallback.
        result = reloaded_lambda_handler(event, {})
        
        assert result['statusCode'] == 200 # Should still succeed, using fallback buckets
        body = json.loads(result['body'])
        assert body['contains_profanity'] is True
        # Should be saved to default flagged-reviews-bucket due to SSM error fallback
        assert 'flagged-reviews-bucket' in body['output_location']

        # Verify DynamoDB was called, even with SSM fallback.
        # This part assumes that the dynamodb client itself can be initialized
        # even if SSM failed for the bucket names.
        mock_customer_profanity_table.update_item.assert_called_once()
        
        mock_open.stop()


if __name__ == '__main__':
    pytest.main([__file__])
