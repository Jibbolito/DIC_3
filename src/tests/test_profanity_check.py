import pytest
import json
from unittest.mock import Mock, patch, create_autospec 
import sys
import os


sys.path.append(os.path.join(os.path.dirname(__file__), '../lambda_functions/profanity_check'))

from lambda_function import check_profanity_in_text, lambda_handler



class TestLambdaHandler:
    """Unit tests for the lambda_handler function"""
    
    

    @patch('lambda_function.s3_client')
    @patch('lambda_function.ssm_client') 
    @patch('lambda_function.dynamodb')   
    def test_lambda_handler_clean_review(self, mock_dynamodb, mock_ssm_client, mock_s3_client):
        """Test lambda execution with clean review"""
        
        mock_ssm_client.get_parameter.side_effect = [
            {'Parameter': {'Value': 'flagged-reviews-bucket'}},
            {'Parameter': {'Value': 'clean-reviews-bucket'}},
            {'Parameter': {'Value': 'CustomerProfanityCounts'}},
            {'Parameter': {'Value': '3'}}
        ]

        
        mock_table = Mock()
        mock_dynamodb.Table.return_value = mock_table
        
        
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

        
        mock_s3_client.exceptions = Mock()
        mock_s3_client.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {}) 
        
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
        
        
        
        with patch('lambda_function._config_loaded', False):
            result = lambda_handler(event, {})
        
        
        assert result['statusCode'] == 200 
        body = json.loads(result['body'])
        print(body)
        assert body['message'] == 'Profanity check completed'
        assert body['contains_profanity'] is False
        assert body['profanity_review_count'] == 0 
        assert 'clean-reviews-bucket' in body['output_location'] 
        
        
        mock_s3_client.get_object.assert_called_once()
        mock_s3_client.put_object.assert_called_once()
        
        
        put_call_args = mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'clean-reviews-bucket' 
        assert 'clean/' in put_call_args[1]['Key']

    @patch('lambda_function.s3_client')
    @patch('lambda_function.ssm_client')
    @patch('lambda_function.dynamodb')
    def test_lambda_handler_profane_review(self, mock_dynamodb, mock_ssm_client, mock_s3_client):
        """Test lambda execution with profane review"""
        mock_ssm_client.get_parameter.side_effect = [
            {'Parameter': {'Value': 'flagged-reviews-bucket'}},
            {'Parameter': {'Value': 'clean-reviews-bucket'}},
            {'Parameter': {'Value': 'CustomerProfanityCounts'}},
            {'Parameter': {'Value': '3'}}
        ]
        mock_table = Mock()
        mock_dynamodb.Table.return_value = mock_table
        mock_table.update_item.return_value = {'Attributes': {'profanity_review_count': 1}}

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

        
        mock_s3_client.exceptions = Mock()
        mock_s3_client.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})
        
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
        
        with patch('lambda_function._config_loaded', False):
            result = lambda_handler(event, {})
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['message'] == 'Profanity check completed'
        assert body['contains_profanity'] is True
        assert body['profanity_review_count'] > 0 
        assert 'flagged-reviews-bucket' in body['output_location']
        
        put_call_args = mock_s3_client.put_object.call_args
        assert put_call_args[1]['Bucket'] == 'flagged-reviews-bucket'
        assert 'flagged/' in put_call_args[1]['Key']
        
        mock_table.update_item.assert_called_once()
    
    @patch('lambda_function.s3_client')
    @patch('lambda_function.ssm_client')
    @patch('lambda_function.dynamodb')
    def test_lambda_handler_missing_fields(self, mock_dynamodb, mock_ssm_client, mock_s3_client):
        """Test lambda execution with missing review fields"""
        mock_ssm_client.get_parameter.side_effect = [
            {'Parameter': {'Value': 'flagged-reviews-bucket'}},
            {'Parameter': {'Value': 'clean-reviews-bucket'}},
            {'Parameter': {'Value': 'CustomerProfanityCounts'}},
            {'Parameter': {'Value': '3'}}
        ]
        mock_table = Mock()
        mock_dynamodb.Table.return_value = mock_table

        mock_review_data = {
            'review_id': 'B001234567',
            'reviewer_id': 'A1234567890'
        }
        
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_review_data).encode('utf-8')))
        }
        mock_s3_client.put_object.return_value = {}

        
        mock_s3_client.exceptions = Mock()
        mock_s3_client.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})
        
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
        
        with patch('lambda_function._config_loaded', False):
            result = lambda_handler(event, {})
        
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['contains_profanity'] is False
        assert body['profanity_review_count'] == 0 
        mock_table.update_item.assert_not_called()
    
    @patch('lambda_function.s3_client')
    @patch('lambda_function.ssm_client')
    @patch('lambda_function.dynamodb')
    def test_lambda_handler_s3_error(self, mock_dynamodb, mock_ssm_client, mock_s3_client):
        """Test lambda execution with S3 error"""
        mock_ssm_client.get_parameter.side_effect = [
            {'Parameter': {'Value': 'flagged-reviews-bucket'}},
            {'Parameter': {'Value': 'clean-reviews-bucket'}},
            {'Parameter': {'Value': 'CustomerProfanityCounts'}},
            {'Parameter': {'Value': '3'}}
        ]
        mock_table = Mock()
        mock_dynamodb.Table.return_value = mock_table

        
        
        
        mock_s3_client.exceptions = Mock()
        mock_s3_client.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})
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
        
        with patch('lambda_function._config_loaded', False):
            result = lambda_handler(event, {})
        
        
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert body['error'] == 'Failed to retrieve or parse review data from S3'
        
        
        mock_table.update_item.assert_not_called()
        mock_s3_client.put_object.assert_not_called()
    
    @patch('lambda_function.s3_client')
    @patch('lambda_function.ssm_client')
    @patch('lambda_function.dynamodb')
    def test_lambda_handler_profanity_analysis_structure(self, mock_dynamodb, mock_ssm_client, mock_s3_client):
        """Test that the profanity analysis structure is correct in output"""
        mock_ssm_client.get_parameter.side_effect = [
            {'Parameter': {'Value': 'flagged-reviews-bucket'}},
            {'Parameter': {'Value': 'clean-reviews-bucket'}},
            {'Parameter': {'Value': 'CustomerProfanityCounts'}},
            {'Parameter': {'Value': '3'}}
        ]
        mock_table = Mock()
        mock_dynamodb.Table.return_value = mock_table

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
        
        
        mock_s3_client.exceptions = Mock()
        mock_s3_client.exceptions.NoSuchKey = type('NoSuchKey', (Exception,), {})

        
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
        
        with patch('lambda_function._config_loaded', False):
            result = lambda_handler(event, {})
        
        sent_data = json.loads(captured_data['Body'])
        
        assert 'profanity_analysis' in sent_data
        profanity_analysis = sent_data['profanity_analysis']
        
        assert 'contains_profanity' in profanity_analysis
        assert 'reviewer_banned' in profanity_analysis
        assert 'current_reviewer_profanity_review_count' in profanity_analysis
        assert 'summary_profanity' in profanity_analysis
        assert 'reviewtext_profanity' in profanity_analysis
        assert 'overall_profanity' in profanity_analysis
        
        assert sent_data['processing_stage'] == 'profanity_checked'

if __name__ == '__main__':
    pytest.main([__file__])