#!/bin/bash

# Lambda Function Deletion Script for LocalStack
# This script deletes specified Lambda functions from your LocalStack instance.

# IMPORTANT: Ensure this script has Unix-style line endings (LF), not Windows (CRLF).
# If you encounter "bad interpreter" errors, run `dos2unix delete_lambdas.sh`

set -e

echo "==================================================="
echo " Starting Lambda Function Deletion               "
echo "==================================================="
echo ""

# Check if LocalStack is running before proceeding
echo "Checking LocalStack status..."
if ! curl -s http://localhost:4566/_localstack/health > /dev/null; then
    echo "ERROR: LocalStack is not running. Please start it first before attempting deletion."
    exit 1
fi
echo "LocalStack is running. Proceeding with Lambda deletion."

# Set AWS CLI to use LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION="us-east-1"
export AWS_ENDPOINT_URL="http://localhost:4566"

# Define the Lambda function names to delete
LAMBDA_FUNCTION_NAMES=(
    "review-preprocessing-dev"
    "review-profanity-check-dev"
    "review-sentiment-analysis-dev"
)

# Function to delete a single Lambda
delete_lambda_function() {
    local function_name=$1

    echo "--- Deleting Lambda: $function_name ---"

    # Check if the Lambda function exists before trying to delete
    set +e # Temporarily disable exit on error for 'get-function'
    aws lambda get-function --function-name "$function_name" --endpoint-url="$AWS_ENDPOINT_URL" > /dev/null 2>&1
    local exists=$?
    set -e # Re-enable exit on error

    if [ $exists -eq 0 ]; then
        echo "  Function '$function_name' found. Deleting..."
        aws lambda delete-function \
            --function-name "$function_name" \
            --endpoint-url="$AWS_ENDPOINT_URL" \
            > /dev/null
        echo "  Function '$function_name' deleted."
    else
        echo "  Function '$function_name' does not exist. Skipping deletion."
    fi
    echo ""
}

# Iterate and delete all defined Lambda functions
for func_name in "${LAMBDA_FUNCTION_NAMES[@]}"; do
    delete_lambda_function "$func_name"
done

echo "==================================================="
echo " Lambda Functions Deletion Completed!            "
echo "==================================================="
echo ""
echo "You can now run ./deploy_lambdas.sh for a fresh deployment of functions."
echo ""