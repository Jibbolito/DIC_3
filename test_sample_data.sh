#!/bin/bash

# Test script using sample data to verify the complete pipeline
# This demonstrates the full workflow with LocalStack

set -e

echo "  Testing Lambda functions with sample data..."

# Configuration
RAW_BUCKET="raw-reviews-bucket"
PROCESSED_BUCKET="processed-reviews-bucket"
CLEAN_BUCKET="clean-reviews-bucket"
FLAGGED_BUCKET="flagged-reviews-bucket"

# Check if LocalStack is running
echo "  Checking LocalStack status..."
if ! curl -s http://localhost:4566/_localstack/health > /dev/null; then
    echo "  LocalStack is not running. Please run:"
    echo "   ./setup_environment.sh"
    exit 1
fi

echo "  LocalStack is running"

# Set AWS CLI to use LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566

echo "  Creating S3 buckets..."

# Create buckets if they don't exist
for bucket in $RAW_BUCKET $PROCESSED_BUCKET $CLEAN_BUCKET $FLAGGED_BUCKET; do
    if awslocal s3 ls s3://$bucket 2>/dev/null; then
        echo "     Bucket $bucket already exists"
    else
        awslocal s3 mb s3://$bucket
        echo "     Created bucket $bucket"
    fi
done

echo "  Uploading sample review files..."

# Upload clean review
awslocal s3 cp data/sample_review.json s3://$RAW_BUCKET/clean_review_test.json
echo "     Uploaded clean review"

# Upload profane review  
awslocal s3 cp data/sample_profane_review.json s3://$RAW_BUCKET/profane_review_test.json
echo "     Uploaded profane review"

echo "  Waiting for processing (if Lambda functions are deployed)..."
sleep 3

echo "  Checking results..."

# Check processed bucket
echo "  Processed bucket contents:"
awslocal s3 ls s3://$PROCESSED_BUCKET/ || echo "     No files found (functions may not be deployed)"

# Check clean bucket
echo "  Clean bucket contents:"
awslocal s3 ls s3://$CLEAN_BUCKET/ || echo "     No files found"

# Check flagged bucket  
echo "  Flagged bucket contents:"
awslocal s3 ls s3://$FLAGGED_BUCKET/ || echo "     No files found"

echo ""
echo "  Manual testing commands:"
echo ""
echo "# List all buckets:"
echo "awslocal s3 ls"
echo ""
echo "# Check bucket contents:"
echo "awslocal s3 ls s3://$RAW_BUCKET/"
echo "awslocal s3 ls s3://$PROCESSED_BUCKET/"
echo "awslocal s3 ls s3://$CLEAN_BUCKET/"
echo "awslocal s3 ls s3://$FLAGGED_BUCKET/"
echo ""
echo "# Download and view processed files:"
echo "awslocal s3 cp s3://$PROCESSED_BUCKET/processed/clean_review_test.json ."
echo "awslocal s3 cp s3://$CLEAN_BUCKET/clean/clean_review_test.json ."
echo ""
echo "# Cleanup test files:"
echo "awslocal s3 rm s3://$RAW_BUCKET/clean_review_test.json"
echo "awslocal s3 rm s3://$RAW_BUCKET/profane_review_test.json"

echo ""
echo "  Sample data testing completed!"
echo "   If Lambda functions are deployed, check the buckets for processed results."