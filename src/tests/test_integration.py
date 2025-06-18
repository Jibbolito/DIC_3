import pytest
import json
import boto3
from moto import mock_s3
import sys
import os
from unittest.mock import patch

# Add lambda function directories to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../lambda_functions/preprocessing'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../lambda_functions/profanity_check'))

import lambda_function as preprocessing_lambda
import lambda_function as profanity_lambda


class TestS3Integration:
    """Integration tests for S3 input/output handling"""
    
    @mock_s3
    def test_preprocessing_s3_integration(self):
        """Test complete S3 integration for preprocessing function"""
        # Setup mock S3
        s3_client = boto3.client('s3', region_name='us-east-1')
        
        # Create buckets
        input_bucket = 'raw-reviews-bucket'
        output_bucket = 'processed-reviews-bucket'
        s3_client.create_bucket(Bucket=input_bucket)
        s3_client.create_bucket(Bucket=output_bucket)
        
        # Create test review data
        review_data = {
            'asin': 'B001234567',
            'reviewerID': 'A1234567890',
            'overall': 5,
            'summary': 'Great product!',
            'reviewText': 'This is an excellent product that I highly recommend to everyone.',
            'unixReviewTime': 1609459200
        }
        
        # Upload test data to input bucket
        input_key = 'test-review.json'
        s3_client.put_object(
            Bucket=input_bucket,
            Key=input_key,
            Body=json.dumps(review_data),
            ContentType='application/json'
        )
        
        # Create S3 event
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
        
        # Mock the s3_client in the lambda function
        with patch.object(preprocessing_lambda, 's3_client', s3_client):
            with patch.dict(os.environ, {'PROCESSED_BUCKET': output_bucket}):
                result = preprocessing_lambda.lambda_handler(event, {})
        
        # Verify successful execution
        assert result['statusCode'] == 200
        
        # Verify output file was created
        expected_output_key = f'processed/{input_key}'
        response = s3_client.get_object(Bucket=output_bucket, Key=expected_output_key)
        output_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Verify output structure
        assert output_data['review_id'] == 'B001234567'
        assert output_data['reviewer_id'] == 'A1234567890'
        assert output_data['processing_stage'] == 'preprocessed'
        assert 'processed_summary' in output_data
        assert 'processed_reviewText' in output_data
        assert 'summary_tokens' in output_data
        assert 'reviewText_tokens' in output_data
        assert output_data['total_word_count'] > 0
    
    @mock_s3
    def test_profanity_check_s3_integration_clean(self):
        """Test S3 integration for profanity check with clean review"""
        # Setup mock S3
        s3_client = boto3.client('s3', region_name='us-east-1')
        
        # Create buckets
        input_bucket = 'processed-reviews-bucket'
        clean_bucket = 'clean-reviews-bucket'
        flagged_bucket = 'flagged-reviews-bucket'
        
        s3_client.create_bucket(Bucket=input_bucket)
        s3_client.create_bucket(Bucket=clean_bucket)
        s3_client.create_bucket(Bucket=flagged_bucket)
        
        # Create processed review data (clean)
        processed_review = {
            'review_id': 'B001234567',
            'reviewer_id': 'A1234567890',
            'overall_rating': 5,
            'processed_summary': 'great product excellent quality',
            'processed_reviewText': 'amazing product highly recommend everyone',
            'processed_overall': '',
            'processing_stage': 'preprocessed'
        }
        
        # Upload to input bucket
        input_key = 'processed/test-review.json'
        s3_client.put_object(
            Bucket=input_bucket,
            Key=input_key,
            Body=json.dumps(processed_review),
            ContentType='application/json'
        )
        
        # Create S3 event
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
        
        # Mock the s3_client in the lambda function
        with patch.object(profanity_lambda, 's3_client', s3_client):
            with patch.dict(os.environ, {
                'CLEAN_BUCKET': clean_bucket,
                'FLAGGED_BUCKET': flagged_bucket
            }):
                result = profanity_lambda.lambda_handler(event, {})
        
        # Verify successful execution
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['contains_profanity'] is False
        
        # Verify output file was created in clean bucket
        clean_objects = s3_client.list_objects_v2(Bucket=clean_bucket)
        assert 'Contents' in clean_objects
        assert len(clean_objects['Contents']) == 1
        
        # Verify no files in flagged bucket
        flagged_objects = s3_client.list_objects_v2(Bucket=flagged_bucket)
        assert 'Contents' not in flagged_objects
        
        # Verify output data structure
        clean_key = clean_objects['Contents'][0]['Key']
        response = s3_client.get_object(Bucket=clean_bucket, Key=clean_key)
        output_data = json.loads(response['Body'].read().decode('utf-8'))
        
        assert output_data['processing_stage'] == 'profanity_checked'
        assert 'profanity_analysis' in output_data
        assert output_data['profanity_analysis']['contains_profanity'] is False
    
    @mock_s3
    def test_profanity_check_s3_integration_flagged(self):
        """Test S3 integration for profanity check with profane review"""
        # Setup mock S3
        s3_client = boto3.client('s3', region_name='us-east-1')
        
        # Create buckets
        input_bucket = 'processed-reviews-bucket'
        clean_bucket = 'clean-reviews-bucket'
        flagged_bucket = 'flagged-reviews-bucket'
        
        s3_client.create_bucket(Bucket=input_bucket)
        s3_client.create_bucket(Bucket=clean_bucket)
        s3_client.create_bucket(Bucket=flagged_bucket)
        
        # Create processed review data (with profanity)
        processed_review = {
            'review_id': 'B001234567',
            'reviewer_id': 'A1234567890',
            'overall_rating': 1,
            'processed_summary': 'shit product terrible quality',
            'processed_reviewText': 'damn awful product waste money',
            'processed_overall': '',
            'processing_stage': 'preprocessed'
        }
        
        # Upload to input bucket
        input_key = 'processed/test-review.json'
        s3_client.put_object(
            Bucket=input_bucket,
            Key=input_key,
            Body=json.dumps(processed_review),
            ContentType='application/json'
        )
        
        # Create S3 event
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
        
        # Mock the s3_client in the lambda function
        with patch.object(profanity_lambda, 's3_client', s3_client):
            with patch.dict(os.environ, {
                'CLEAN_BUCKET': clean_bucket,
                'FLAGGED_BUCKET': flagged_bucket
            }):
                result = profanity_lambda.lambda_handler(event, {})
        
        # Verify successful execution
        assert result['statusCode'] == 200
        body = json.loads(result['body'])
        assert body['contains_profanity'] is True
        assert body['profanity_count'] > 0
        
        # Verify output file was created in flagged bucket
        flagged_objects = s3_client.list_objects_v2(Bucket=flagged_bucket)
        assert 'Contents' in flagged_objects
        assert len(flagged_objects['Contents']) == 1
        
        # Verify no files in clean bucket
        clean_objects = s3_client.list_objects_v2(Bucket=clean_bucket)
        assert 'Contents' not in clean_objects
        
        # Verify output data structure
        flagged_key = flagged_objects['Contents'][0]['Key']
        response = s3_client.get_object(Bucket=flagged_bucket, Key=flagged_key)
        output_data = json.loads(response['Body'].read().decode('utf-8'))
        
        assert output_data['processing_stage'] == 'profanity_checked'
        assert 'profanity_analysis' in output_data
        assert output_data['profanity_analysis']['contains_profanity'] is True
        assert len(output_data['profanity_analysis']['profanity_words']) > 0
    
    @mock_s3
    def test_end_to_end_pipeline(self):
        """Test end-to-end pipeline: preprocessing -> profanity check"""
        # Setup mock S3
        s3_client = boto3.client('s3', region_name='us-east-1')
        
        # Create all required buckets
        raw_bucket = 'raw-reviews-bucket'
        processed_bucket = 'processed-reviews-bucket'
        clean_bucket = 'clean-reviews-bucket'
        
        for bucket in [raw_bucket, processed_bucket, clean_bucket]:
            s3_client.create_bucket(Bucket=bucket)
        
        # Create raw review data
        raw_review = {
            'asin': 'B001234567',
            'reviewerID': 'A1234567890',
            'overall': 5,
            'summary': 'Great product!',
            'reviewText': 'This is an excellent product with amazing quality.',
            'unixReviewTime': 1609459200
        }
        
        # Step 1: Upload raw review
        raw_key = 'test-review.json'
        s3_client.put_object(
            Bucket=raw_bucket,
            Key=raw_key,
            Body=json.dumps(raw_review),
            ContentType='application/json'
        )
        
        # Step 2: Run preprocessing
        preprocessing_event = {
            'Records': [{'s3': {'bucket': {'name': raw_bucket}, 'object': {'key': raw_key}}}]
        }
        
        with patch.object(preprocessing_lambda, 's3_client', s3_client):
            with patch.dict(os.environ, {'PROCESSED_BUCKET': processed_bucket}):
                preprocessing_result = preprocessing_lambda.lambda_handler(preprocessing_event, {})
        
        assert preprocessing_result['statusCode'] == 200
        
        # Step 3: Verify processed file exists
        processed_key = f'processed/{raw_key}'
        processed_objects = s3_client.list_objects_v2(Bucket=processed_bucket)
        assert 'Contents' in processed_objects
        
        # Step 4: Run profanity check on processed file
        profanity_event = {
            'Records': [{'s3': {'bucket': {'name': processed_bucket}, 'object': {'key': processed_key}}}]
        }
        
        with patch.object(profanity_lambda, 's3_client', s3_client):
            with patch.dict(os.environ, {'CLEAN_BUCKET': clean_bucket, 'FLAGGED_BUCKET': 'flagged-bucket'}):
                profanity_result = profanity_lambda.lambda_handler(profanity_event, {})
        
        assert profanity_result['statusCode'] == 200
        
        # Step 5: Verify final file in clean bucket (since no profanity)
        clean_objects = s3_client.list_objects_v2(Bucket=clean_bucket)
        assert 'Contents' in clean_objects
        
        # Verify final output structure
        clean_key = clean_objects['Contents'][0]['Key']
        response = s3_client.get_object(Bucket=clean_bucket, Key=clean_key)
        final_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Should contain data from both processing stages
        assert final_data['review_id'] == 'B001234567'
        assert final_data['processing_stage'] == 'profanity_checked'
        assert 'processed_summary' in final_data
        assert 'processed_reviewText' in final_data
        assert 'profanity_analysis' in final_data
        assert final_data['profanity_analysis']['contains_profanity'] is False
    
    @mock_s3
    def test_error_handling_missing_bucket(self):
        """Test error handling when S3 bucket doesn't exist"""
        # Setup mock S3 but don't create the output bucket
        s3_client = boto3.client('s3', region_name='us-east-1')
        
        input_bucket = 'existing-bucket'
        s3_client.create_bucket(Bucket=input_bucket)
        
        # Create test data
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
        
        # Try to run preprocessing with non-existent output bucket
        with patch.object(preprocessing_lambda, 's3_client', s3_client):
            with patch.dict(os.environ, {'PROCESSED_BUCKET': 'non-existent-bucket'}):
                result = preprocessing_lambda.lambda_handler(event, {})
        
        # Should return error
        assert result['statusCode'] == 500
        body = json.loads(result['body'])
        assert 'error' in body


if __name__ == '__main__':
    pytest.main([__file__])