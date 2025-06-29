import pytest
import json
import boto3
from moto import mock_aws
import sys
import os
from unittest.mock import patch
import importlib.util 


PREPROCESSING_LAMBDA_DIR = os.path.join(os.path.dirname(__file__), '../lambda_functions/preprocessing')
PROFANITY_LAMBDA_DIR = os.path.join(os.path.dirname(__file__), '../lambda_functions/profanity_check')





preprocessing_lambda_path = os.path.join(PREPROCESSING_LAMBDA_DIR, 'lambda_function.py')
spec_pre = importlib.util.spec_from_file_location("preprocessing_lambda_module", preprocessing_lambda_path)
preprocessing_lambda = importlib.util.module_from_spec(spec_pre)

sys.modules["preprocessing_lambda_module"] = preprocessing_lambda 
spec_pre.loader.exec_module(preprocessing_lambda)


profanity_lambda_path = os.path.join(PROFANITY_LAMBDA_DIR, 'lambda_function.py')
spec_prof = importlib.util.spec_from_file_location("profanity_lambda_module", profanity_lambda_path)
profanity_lambda = importlib.util.module_from_spec(spec_prof)

sys.modules["profanity_lambda_module"] = profanity_lambda 
spec_prof.loader.exec_module(profanity_lambda)


class TestS3Integration:
    """Integration tests for S3 input/output handling"""
    
    @mock_aws
    def test_preprocessing_s3_integration(self):
        """Test complete S3 integration for preprocessing function"""
        
        s3_client = boto3.client('s3', region_name='us-east-1')
        
        
        input_bucket = 'raw-reviews-bucket'
        output_bucket = 'processed-reviews-bucket'
        s3_client.create_bucket(Bucket=input_bucket)
        s3_client.create_bucket(Bucket=output_bucket)
        
        
        review_data = {
            'asin': 'B001234567',
            'reviewerID': 'A1234567890',
            'overall': 5,
            'summary': 'Great product!',
            'reviewText': 'This is an excellent product that I highly recommend to everyone.',
            'unixReviewTime': 1609459200
        }
        
        
        input_key = 'test-review.json'
        s3_client.put_object(
            Bucket=input_bucket,
            Key=input_key,
            Body=json.dumps(review_data),
            ContentType='application/json'
        )
        
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': input_bucket},
                        'object': {'key': input_key}
                    }
                }
            ]
        }
        
        
        with patch.object(preprocessing_lambda, 's3_client', s3_client):
            with patch.dict(os.environ, {'PROCESSED_BUCKET': output_bucket}):
                result = preprocessing_lambda.lambda_handler(event, {})
        
        
        assert result['statusCode'] == 200
        
        
        
        review_id = f"{review_data['asin']}{review_data['reviewerID']}"
        expected_output_key = f"processed/{os.path.splitext(input_key)[0]}/{review_id}.json"
        
        response = s3_client.get_object(Bucket=output_bucket, Key=expected_output_key)
        saved_data = json.loads(response['Body'].read().decode('utf-8'))
        
        
        assert saved_data['review_id'] == review_id
        assert saved_data['processing_stage'] == 'preprocessed'
        assert saved_data['summary_word_count'] == 2
        assert saved_data['reviewText_word_count'] == 5 


    @mock_aws
    def test_profanity_check_s3_integration_clean(self):
        """Test S3 integration for profanity check with clean review"""
        
        s3_client = boto3.client('s3', region_name='us-east-1')
        ssm_client_mock = boto3.client('ssm', region_name='us-east-1')
        dynamodb_mock_resource = boto3.resource('dynamodb', region_name='us-east-1')
        
        
        input_bucket = 'processed-reviews-bucket'
        clean_bucket = 'clean-reviews-bucket'
        flagged_bucket = 'flagged-reviews-bucket'
        
        s3_client.create_bucket(Bucket=input_bucket)
        s3_client.create_bucket(Bucket=clean_bucket)
        s3_client.create_bucket(Bucket=flagged_bucket)

        
        ssm_client_mock.put_parameter(Name='/my-app/s3/clean_bucket_name', Value=clean_bucket, Type='String', Overwrite=True)
        ssm_client_mock.put_parameter(Name='/my-app/s3/flagged_bucket_name', Value=flagged_bucket, Type='String', Overwrite=True)
        ssm_client_mock.put_parameter(Name='/my-app/dynamodb/customer_profanity_table_name', Value='MockCustomerProfanityCounts', Type='String', Overwrite=True)
        ssm_client_mock.put_parameter(Name='/my-app/ban_threshold', Value='3', Type='String', Overwrite=True)
        
        
        dynamodb_mock_resource.create_table(
            TableName='MockCustomerProfanityCounts',
            KeySchema=[{'AttributeName': 'reviewer_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'reviewer_id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
        )

        
        processed_review = {
            'asin': 'B001234567',  
            'reviewerID': 'A1234567890', 
            'review_id': 'B001234567A1234567890', 
            'reviewer_id': 'A1234567890',
            'overall_rating': 5,
            'processed_summary': 'great product excellent quality',
            'processed_reviewText': 'amazing product highly recommend everyone',
            'processed_overall': '',
            'processing_stage': 'preprocessed'
        }
        
        
        
        review_id_for_processed_file = processed_review['review_id']
        input_filename_for_profanity = 'test-review.json' 
        original_processed_key = f"processed/{os.path.splitext(input_filename_for_profanity)[0]}/{review_id_for_processed_file}.json"
        
        s3_client.put_object(
            Bucket=input_bucket,
            Key=original_processed_key,
            Body=json.dumps(processed_review),
            ContentType='application/json'
        )
        
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': input_bucket},
                        'object': {'key': original_processed_key}
                    }
                }
            ]
        }
        
        
        with patch.object(profanity_lambda, 's3_client', s3_client), \
             patch.object(profanity_lambda, 'ssm_client', ssm_client_mock), \
             patch.object(profanity_lambda, 'dynamodb', dynamodb_mock_resource):
            
            
            profanity_lambda._config_loaded = False
            profanity_lambda.load_config() 

            
            result = profanity_lambda.lambda_handler(event, {})
        
        
        assert result['statusCode'] == 200
        
        
        expected_clean_key = f"clean/{processed_review['review_id']}.json"

        
        try:
            clean_object = s3_client.get_object(Bucket=clean_bucket, Key=expected_clean_key)
            clean_data = json.loads(clean_object['Body'].read().decode('utf-8'))
            assert clean_data['profanity_analysis']['contains_profanity'] is False
            
            
            assert 'summary_profanity' in clean_data['profanity_analysis']
            assert 'reviewtext_profanity' in clean_data['profanity_analysis']
            assert clean_data['profanity_analysis']['summary_profanity']['contains_profanity'] is False
            assert clean_data['profanity_analysis']['reviewtext_profanity']['contains_profanity'] is False

            
            
            
            assert clean_data['processing_stage'] == 'profanity_checked' 
        except s3_client.exceptions.NoSuchKey:
            pytest.fail(f"File {expected_clean_key} not found in clean bucket {clean_bucket}")
        
        
        expected_flagged_key = f"flagged/{processed_review['review_id']}.json"
        with pytest.raises(s3_client.exceptions.NoSuchKey):
            s3_client.get_object(Bucket=flagged_bucket, Key=expected_flagged_key)


    @mock_aws
    def test_profanity_check_s3_integration_flagged(self):
        """Test S3 integration for profanity check with profane review"""
        
        s3_client = boto3.client('s3', region_name='us-east-1')
        ssm_client_mock = boto3.client('ssm', region_name='us-east-1')
        dynamodb_mock_resource = boto3.resource('dynamodb', region_name='us-east-1')
        
        
        input_bucket = 'processed-reviews-bucket'
        clean_bucket = 'clean-reviews-bucket'
        flagged_bucket = 'flagged-reviews-bucket'
        
        s3_client.create_bucket(Bucket=input_bucket)
        s3_client.create_bucket(Bucket=clean_bucket)
        s3_client.create_bucket(Bucket=flagged_bucket)

        
        ssm_client_mock.put_parameter(Name='/my-app/s3/clean_bucket_name', Value=clean_bucket, Type='String', Overwrite=True)
        ssm_client_mock.put_parameter(Name='/my-app/s3/flagged_bucket_name', Value=flagged_bucket, Type='String', Overwrite=True)
        ssm_client_mock.put_parameter(Name='/my-app/dynamodb/customer_profanity_table_name', Value='MockCustomerProfanityCounts', Type='String', Overwrite=True)
        ssm_client_mock.put_parameter(Name='/my-app/ban_threshold', Value='3', Type='String', Overwrite=True)
        
        
        dynamodb_mock_resource.create_table(
            TableName='MockCustomerProfanityCounts',
            KeySchema=[{'AttributeName': 'reviewer_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'reviewer_id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
        )

        
        processed_review = {
            'asin': 'B001234567', 
            'reviewerID': 'A1234567890', 
            'review_id': 'B001234567A1234567890',
            'reviewer_id': 'A1234567890',
            'overall_rating': 1,
            'processed_summary': 'shit product terrible quality',
            'processed_reviewText': 'damn awful product waste money',
            'processed_overall': '',
            'processing_stage': 'preprocessed'
        }
        
        
        review_id_for_processed_file = processed_review['review_id']
        input_filename_for_profanity = 'test-review.json' 
        original_processed_key = f"processed/{os.path.splitext(input_filename_for_profanity)[0]}/{review_id_for_processed_file}.json"
        
        s3_client.put_object(
            Bucket=input_bucket,
            Key=original_processed_key,
            Body=json.dumps(processed_review),
            ContentType='application/json'
        )
        
        
        event = {
            'Records': [
                {
                    's3': {
                        'bucket': {'name': input_bucket},
                        'object': {'key': original_processed_key}
                    }
                }
            ]
        }
        
        
        with patch.object(profanity_lambda, 's3_client', s3_client), \
             patch.object(profanity_lambda, 'ssm_client', ssm_client_mock), \
             patch.object(profanity_lambda, 'dynamodb', dynamodb_mock_resource):

            
            profanity_lambda._config_loaded = False
            profanity_lambda.load_config() 

            
            result = profanity_lambda.lambda_handler(event, {})
        
        
        assert result['statusCode'] == 200
        
        
        expected_flagged_key = f"flagged/{processed_review['review_id']}.json"

        
        try:
            flagged_object = s3_client.get_object(Bucket=flagged_bucket, Key=expected_flagged_key)
            flagged_data = json.loads(flagged_object['Body'].read().decode('utf-8'))
            assert flagged_data['profanity_analysis']['contains_profanity'] is True
            
            assert 'summary_profanity' in flagged_data['profanity_analysis']
            assert 'reviewtext_profanity' in flagged_data['profanity_analysis']
            
            
            assert flagged_data['profanity_analysis']['summary_profanity']['contains_profanity'] is True
            assert flagged_data['profanity_analysis']['reviewtext_profanity']['contains_profanity'] is True

            
            assert flagged_data['profanity_analysis']['summary_profanity']['censored_text'] == '**** product terrible quality'
            assert flagged_data['profanity_analysis']['reviewtext_profanity']['censored_text'] == '**** awful product waste money'
            
            
            
            assert 'overall_profanity' in flagged_data['profanity_analysis']
            assert flagged_data['profanity_analysis']['overall_profanity']['contains_profanity'] is False
            
            assert flagged_data['processing_stage'] == 'profanity_checked' 
        except s3_client.exceptions.NoSuchKey:
            pytest.fail(f"File {expected_flagged_key} not found in flagged bucket {flagged_bucket}")
            
        
        expected_clean_key = f"clean/{processed_review['review_id']}.json"
        with pytest.raises(s3_client.exceptions.NoSuchKey):
            s3_client.get_object(Bucket=clean_bucket, Key=expected_clean_key)


    @mock_aws
    def test_end_to_end_pipeline(self):
        """Test end-to-end pipeline: preprocessing -> profanity check"""
        
        s3_client = boto3.client('s3', region_name='us-east-1')
        ssm_client_mock = boto3.client('ssm', region_name='us-east-1')
        dynamodb_mock_resource = boto3.resource('dynamodb', region_name='us-east-1')
        
        
        raw_bucket = 'raw-reviews-bucket'
        processed_bucket = 'processed-reviews-bucket'
        clean_bucket = 'clean-reviews-bucket'
        flagged_bucket = 'flagged-reviews-bucket'
        
        for bucket in [raw_bucket, processed_bucket, clean_bucket, flagged_bucket]:
            s3_client.create_bucket(Bucket=bucket)

        
        ssm_client_mock.put_parameter(Name='/my-app/s3/clean_bucket_name', Value=clean_bucket, Type='String', Overwrite=True)
        ssm_client_mock.put_parameter(Name='/my-app/s3/flagged_bucket_name', Value=flagged_bucket, Type='String', Overwrite=True)
        ssm_client_mock.put_parameter(Name='/my-app/dynamodb/customer_profanity_table_name', Value='MockCustomerProfanityCounts', Type='String', Overwrite=True)
        ssm_client_mock.put_parameter(Name='/my-app/ban_threshold', Value='3', Type='String', Overwrite=True)

        
        dynamodb_mock_resource.create_table(
            TableName='MockCustomerProfanityCounts',
            KeySchema=[{'AttributeName': 'reviewer_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'reviewer_id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 1, 'WriteCapacityUnits': 1}
        )
        
        
        raw_review = {
            'asin': 'B001234567',
            'reviewerID': 'A1234567890',
            'overall': 5,
            'summary': 'Great product!',
            'reviewText': 'This is an excellent product with amazing quality.',
            'unixReviewTime': 1609459200
        }
        
        
        raw_key = 'test-review.json'
        s3_client.put_object(
            Bucket=raw_bucket,
            Key=raw_key,
            Body=json.dumps(raw_review),
            ContentType='application/json'
        )
        
        
        preprocessing_event = {
            'Records': [{'s3': {'bucket': {'name': raw_bucket}, 'object': {'key': raw_key}}}]
        }
        
        with patch.object(preprocessing_lambda, 's3_client', s3_client):
            with patch.dict(os.environ, {'PROCESSED_BUCKET': processed_bucket}):
                preprocessing_result = preprocessing_lambda.lambda_handler(preprocessing_event, {})
        
        assert preprocessing_result['statusCode'] == 200
        
        
        
        review_id_from_preprocessing = f"{raw_review['asin']}{raw_review['reviewerID']}"
        processed_key = f"processed/{os.path.splitext(raw_key)[0]}/{review_id_from_preprocessing}.json"
        
        processed_object = s3_client.get_object(Bucket=processed_bucket, Key=processed_key)
        processed_data = json.loads(processed_object['Body'].read().decode('utf-8'))
        assert processed_data['processing_stage'] == 'preprocessed'
        
        
        profanity_event = {
            'Records': [{'s3': {'bucket': {'name': processed_bucket}, 'object': {'key': processed_key}}}]
        }
        
        with patch.object(profanity_lambda, 's3_client', s3_client), \
             patch.object(profanity_lambda, 'ssm_client', ssm_client_mock), \
             patch.object(profanity_lambda, 'dynamodb', dynamodb_mock_resource):

            
            profanity_lambda._config_loaded = False
            profanity_lambda.load_config() 

            profanity_result = profanity_lambda.lambda_handler(profanity_event, {})
        
        assert profanity_result['statusCode'] == 200
        
        
        
        final_expected_key = f"clean/{review_id_from_preprocessing}.json"

        try:
            final_object = s3_client.get_object(Bucket=clean_bucket, Key=final_expected_key)
            final_data = json.loads(final_object['Body'].read().decode('utf-8'))
            assert final_data['processing_stage'] == 'profanity_checked' 
            assert 'profanity_analysis' in final_data
            assert final_data['profanity_analysis']['contains_profanity'] is False
        except s3_client.exceptions.NoSuchKey:
            pytest.fail(f"Final processed file {final_expected_key} not found in clean bucket {clean_bucket}")
        
        
        final_expected_flagged_key = f"flagged/{review_id_from_preprocessing}.json"
        with pytest.raises(s3_client.exceptions.NoSuchKey):
            s3_client.get_object(Bucket=flagged_bucket, Key=final_expected_flagged_key)
    
    @mock_aws
    def test_error_handling_missing_bucket(self):
        """Test error handling when S3 bucket doesn't exist"""
        
        s3_client = boto3.client('s3', region_name='us-east-1')
        
        input_bucket = 'existing-bucket'
        s3_client.create_bucket(Bucket=input_bucket)
        
        
        review_data = {'asin': 'B001234567', 'summary': 'Test'}
        input_key = 'test-review.json'
        s3_client.put_object(
            Bucket=input_bucket,
            Key=input_key,
            Body=json.dumps(review_data),
            ContentType='application/json'
        )
        
        event = {
            'Records': [{'s3': {'bucket': {'name': input_bucket}, 'object': {'key': input_key}}}]
        }
        
        
        with patch.object(preprocessing_lambda, 's3_client', s3_client):
            with patch.dict(os.environ, {'PROCESSED_BUCKET': 'non-existent-bucket'}):
                result = preprocessing_lambda.lambda_handler(event, {})
        
        
        assert result['statusCode'] == 500
        assert "Failed to preprocess reviews" in result['body']

if __name__ == '__main__':
    pytest.main([__file__])
