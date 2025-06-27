#!/bin/bash

echo "==================================================="
echo " Lambda Functions Deployment Started!            "
echo "==================================================="
echo ""

# Set AWS CLI to use LocalStack (important for these commands)
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION="us-east-1"
export AWS_ENDPOINT_URL="http://localhost:4566"

# Define Lambda function names
FILE_SPLITTER_FUNCTION="review-file-splitter-dev"
PREPROCESSING_FUNCTION="review-preprocessing-dev"
PROFANITY_CHECK_FUNCTION="review-profanity-check-dev"
SENTIMENT_ANALYSIS_FUNCTION="review-sentiment-analysis-dev"

# Define bucket names (retrieved from SSM, or fallbacks if SSM fails, as in your Python code)
# For deployment, we assume these are already set up by setup_eventbridge.sh
# and are consistent with your SSM parameters and Python code's expectations.
RAW_BUCKET="raw-reviews-bucket"
SPLIT_BUCKET="split-reviews-bucket"
PROCESSED_BUCKET="processed-reviews-bucket"
CLEAN_BUCKET="clean-reviews-bucket"
FLAGGED_BUCKET="flagged-reviews-bucket"
FINAL_REVIEWS_BUCKET="final-reviews-bucket"

# Delete existing functions first to ensure a clean deployment
echo "Cleaning up existing functions (if any)..."
# Using 2>/dev/null || true to suppress "function not found" errors if they don't exist
awslocal lambda delete-function --function-name "$FILE_SPLITTER_FUNCTION" --endpoint-url="$AWS_ENDPOINT_URL" 2>/dev/null || true
awslocal lambda delete-function --function-name "$PREPROCESSING_FUNCTION" --endpoint-url="$AWS_ENDPOINT_URL" 2>/dev/null || true
awslocal lambda delete-function --function-name "$PROFANITY_CHECK_FUNCTION" --endpoint-url="$AWS_ENDPOINT_URL" 2>/dev/null || true
awslocal lambda delete-function --function-name "$SENTIMENT_ANALYSIS_FUNCTION" --endpoint-url="$AWS_ENDPOINT_URL" 2>/dev/null || true
sleep 3 # Give LocalStack a moment to process deletions

# Deploy File Splitter function (NEW)
echo "Creating file splitter function..."
awslocal lambda create-function \
    --function-name "$FILE_SPLITTER_FUNCTION" \
    --runtime python3.10 \
    --zip-file fileb://deployments/splitter_deployment.zip \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 600 \
    --memory-size 2048 \
    --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566,RAW_BUCKET=$RAW_BUCKET,SPLIT_BUCKET=$SPLIT_BUCKET}" \
    --endpoint-url="$AWS_ENDPOINT_URL"

echo "Creating preprocessing function..."
awslocal lambda create-function \
    --function-name "$PREPROCESSING_FUNCTION" \
    --runtime python3.10 \
    --zip-file fileb://deployments/preprocessing_deployment.zip \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 300 \
    --memory-size 4096 \
    --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566,PROCESSED_BUCKET=$PROCESSED_BUCKET}" \
    --endpoint-url="$AWS_ENDPOINT_URL"

echo "Creating profanity check function..."
awslocal lambda create-function \
    --function-name "$PROFANITY_CHECK_FUNCTION" \
    --runtime python3.10 \
    --zip-file fileb://deployments/profanity_check_deployment.zip \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 600 \
    --memory-size 4096 \
    --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566,FLAGGED_BUCKET=$FLAGGED_BUCKET,CLEAN_BUCKET=$CLEAN_BUCKET,CUSTOMER_PROFANITY_TABLE_NAME=CustomerProfanityCounts,BAN_THRESHOLD_VALUE=3}" \
    --endpoint-url="$AWS_ENDPOINT_URL"

echo "Creating sentiment analysis function..."
awslocal lambda create-function \
    --function-name "$SENTIMENT_ANALYSIS_FUNCTION" \
    --runtime python3.10 \
    --zip-file fileb://deployments/sentiment_analysis_deployment.zip \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 900 \
    --memory-size 4096 \
    --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566,FINAL_REVIEWS_BUCKET=$FINAL_REVIEWS_BUCKET}" \
    --endpoint-url="$AWS_ENDPOINT_URL"

echo "==================================================="
echo " Lambda Functions Deployment Completed!          "
echo "==================================================="
echo ""
