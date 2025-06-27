#!/bin/bash

# AWS Resources Setup Script
# This script automates:
# 1. AWS S3 bucket creation and SSM parameter registration
# 2. AWS DynamoDB table creation and SSM parameter registration (for profanity counts)
# 3. AWS EventBridge rule and target creation to chain Lambda functions via S3 events

set -e # Exit immediately if a command exits with a non-zero status
set -x # Print commands and their arguments as they are executed (for debugging)

echo "==================================================="
echo " Starting AWS Resources Setup                    "
echo "==================================================="
echo ""

# Check if LocalStack is running before proceeding
echo "📡 Checking LocalStack status... (This may take a moment)"
# Allow LocalStack to fully start up and health check before proceeding
timeout 150 bash -c 'until curl -s http://localhost:4566/_localstack/health | grep -q "running"; do echo -n "."; sleep 1; done' || \
{ echo "   ERROR: LocalStack is not running or did not start within 150 seconds. Please ensure it's running and accessible."; exit 1; }
echo "   LocalStack is running. Proceeding with AWS resource creation."

# Set AWS CLI to use LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION="us-east-1"
export AWS_ENDPOINT_URL="http://localhost:4566"

echo ""
echo "--- Section 1: AWS Resource Creation (S3 Buckets, DynamoDB, SSM Parameters) ---"

# Define resource names
RAW_BUCKET="raw-reviews-bucket"
SPLIT_BUCKET="split-reviews-bucket" # NEW BUCKET FOR SPLIT FILES
PROCESSED_BUCKET="processed-reviews-bucket"
CLEAN_BUCKET="clean-reviews-bucket"
FLAGGED_BUCKET="flagged-reviews-bucket"
FINAL_REVIEWS_BUCKET="final-reviews-bucket" # This is the target for sentiment analysis
CUSTOMER_PROFANITY_TABLE_NAME="CustomerProfanityCounts"
BAN_THRESHOLD_VALUE="3"

# Define Lambda function names (ensure these match your setup_aws_lambdas.sh)
FILE_SPLITTER_FUNCTION="review-file-splitter-dev" # NEW LAMBDA FUNCTION
PREPROCESSING_FUNCTION="review-preprocessing-dev"
PROFANITY_CHECK_FUNCTION="review-profanity-check-dev"
SENTIMENT_ANALYSIS_FUNCTION="review-sentiment-analysis-dev"

# Helper function to delete S3 bucket (more robust)
delete_s3_bucket() {
    local bucket_name=$1
    echo "   Attempting to delete bucket: $bucket_name..."
    # First, try to delete bucket notification configuration explicitly
    awslocal s3api delete-bucket-notification --bucket "$bucket_name" 2>/dev/null || true
    sleep 1 # Short pause after deleting notification config

    # Then, try to delete all objects in the bucket
    awslocal s3 rm "s3://$bucket_name" --recursive 2>/dev/null || true # Ignore errors if bucket doesn't exist or is empty
    # Finally, try to delete the bucket itself
    awslocal s3 rb "s3://$bucket_name" --force 2>/dev/null && echo "   Bucket $bucket_name deleted." || echo "   Bucket $bucket_name did not exist or could not be deleted."
}

# Delete existing buckets for a clean slate
echo "Ensuring S3 buckets are clean..."
delete_s3_bucket "$RAW_BUCKET"
delete_s3_bucket "$SPLIT_BUCKET" # Delete new bucket
delete_s3_bucket "$PROCESSED_BUCKET"
delete_s3_bucket "$CLEAN_BUCKET"
delete_s3_bucket "$FLAGGED_BUCKET"
delete_s3_bucket "$FINAL_REVIEWS_BUCKET"
sleep 7 # Give more time for buckets to be fully deleted before recreating

# Helper function to create S3 bucket
create_s3_bucket() {
    local bucket_name=$1
    echo "   Creating S3 bucket: $bucket_name..."
    awslocal s3 mb "s3://$bucket_name" && echo "   Bucket $bucket_name created." || echo "   Bucket $bucket_name already exists or could not be created."
}

# Create S3 buckets
create_s3_bucket "$RAW_BUCKET"
create_s3_bucket "$SPLIT_BUCKET" # Create new bucket
create_s3_bucket "$PROCESSED_BUCKET"
create_s3_bucket "$CLEAN_BUCKET"
create_s3_bucket "$FLAGGED_BUCKET"
create_s3_bucket "$FINAL_REVIEWS_BUCKET"
echo "   All S3 buckets checked/created."
sleep 7 # Give more time for S3 buckets to be fully propagated before attempting notification config or put-parameter

# Register S3 bucket names in SSM Parameter Store
echo "Registering S3 bucket names in SSM Parameter Store..."
awslocal ssm put-parameter --name "/my-app/s3/raw_bucket_name" --value "$RAW_BUCKET" --type "String" --overwrite
awslocal ssm put-parameter --name "/my-app/s3/split_bucket_name" --value "$SPLIT_BUCKET" --type "String" --overwrite # Register new bucket
awslocal ssm put-parameter --name "/my-app/s3/processed_bucket_name" --value "$PROCESSED_BUCKET" --type "String" --overwrite
awslocal ssm put-parameter --name "/my-app/s3/clean_bucket_name" --value "$CLEAN_BUCKET" --type "String" --overwrite
awslocal ssm put-parameter --name "/my-app/s3/flagged_bucket_name" --value "$FLAGGED_BUCKET" --type "String" --overwrite
awslocal ssm put-parameter --name "/my-app/s3/final_reviews_bucket_name" --value "$FINAL_REVIEWS_BUCKET" --type "String" --overwrite
echo "   SSM parameters for S3 buckets created."
sleep 5 # Small pause after SSM puts

# Create DynamoDB table
echo "Creating DynamoDB table: $CUSTOMER_PROFANITY_TABLE_NAME..."
awslocal dynamodb create-table \
    --table-name "$CUSTOMER_PROFANITY_TABLE_NAME" \
    --attribute-definitions \
        AttributeName=reviewer_id,AttributeType=S \
    --key-schema \
        AttributeName=reviewer_id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    2>/dev/null && echo "   DynamoDB table '$CUSTOMER_PROFANITY_TABLE_NAME' created." || echo "   DynamoDB table '$CUSTOMER_PROFANITY_TABLE_NAME' already exists or could not be created."
echo "   DynamoDB table '$CUSTOMER_PROFANITY_TABLE_NAME' checked/created."
sleep 5 # Small pause after DDB table creation

# Create SSM Parameters for DynamoDB table and ban threshold
echo "   Creating SSM Parameters for DynamoDB table and ban threshold..."
awslocal ssm put-parameter \
    --name "/my-app/dynamodb/customer_profanity_table_name" \
    --type "String" \
    --value "$CUSTOMER_PROFANITY_TABLE_NAME" \
    --overwrite
awslocal ssm put-parameter \
    --name "/my-app/ban_threshold" \
    --type "String" \
    --value "$BAN_THRESHOLD_VALUE" \
    --overwrite
echo "   SSM parameters for DynamoDB and ban threshold created."
sleep 5 # Small pause after SSM puts

echo ""
echo "--- Section 2: EventBridge Setup for Lambda Triggers ---"

# Helper function to create S3-to-Lambda trigger via EventBridge
setup_s3_eventbridge_trigger() {
    local bucket_name=$1
    local lambda_function_name=$2
    local rule_name_prefix=$3
    local region=$AWS_DEFAULT_REGION # Use exported region
    local trigger_name="${rule_name_prefix}-${lambda_function_name}-trigger"

    echo "   Setting up trigger for $lambda_function_name on $bucket_name (Rule: $trigger_name)..."

    # Delete existing rule to avoid conflicts and ensure fresh config
    awslocal events delete-rule --name "$trigger_name" 2>/dev/null && echo "      Existing rule $trigger_name deleted." || echo "      Rule $trigger_name did not exist."
    sleep 3 # Give more time for rule deletion to propagate

    # IMPORTANT: Explicitly delete any existing bucket notification configuration first
    # and then re-enable EventBridge notification configuration.
    # This is often the key to forcing LocalStack to clear stale S3->EventBridge links.
    echo "      Deleting existing notification configuration for bucket: $bucket_name..."
    awslocal s3api delete-bucket-notification --bucket "$bucket_name" 2>/dev/null || true
    sleep 2 # Short pause

    echo "      Enabling EventBridge for bucket: $bucket_name..."
    awslocal s3api put-bucket-notification-configuration \
        --bucket "$bucket_name" \
        --notification-configuration '{"EventBridgeConfiguration": {}}' \
        || echo "      Error enabling EventBridge for $bucket_name."
    sleep 5 # Give significant time for notification configuration to propagate

    # Event pattern as a JSON string
    local event_pattern="{\"source\":[\"aws.s3\"],\"detail-type\":[\"Object Created\"],\"detail\":{\"bucket\":{\"name\":[\"${bucket_name}\"]}}}"

    # Create EventBridge rule
    echo "      Creating EventBridge rule: $trigger_name..."
    awslocal events put-rule \
        --name "$trigger_name" \
        --event-pattern "$event_pattern" \
        --state ENABLED
    sleep 3 # Give more time for rule creation to propagate

    # Get function ARN - add a sleep before this too
    echo "      Getting ARN for function: $lambda_function_name..."
    sleep 3 # Give lambda service time to register function fully
    local function_arn
    if command -v jq &> /dev/null; then
        function_arn=$(awslocal lambda get-function --function-name "$lambda_function_name" | jq -r .Configuration.FunctionArn)
    else
        echo "      jq not found. Please install jq for robust ARN retrieval (e.g., brew install jq or apt-get install jq)."
        echo "      Attempting manual parsing, which may be less reliable."
        function_arn=$(awslocal lambda get-function --function-name "$lambda_function_name" | grep -o '"FunctionArn":"[^"]*"' | cut -d'"' -f4)
    fi

    if [ "$function_arn" == "null" ] || [ -z "$function_arn" ]; then
        echo "   ERROR: Could not find function: $lambda_function_name. Please ensure it's deployed before setting up triggers."
        exit 1
    fi
    echo "      Found function ARN: $function_arn"

    # Add Lambda function as target
    echo "      Adding Lambda function as target for rule: $trigger_name..."
    awslocal events put-targets \
        --rule "$trigger_name" \
        --targets "Id=${lambda_function_name}-target",Arn="$function_arn"
    sleep 3 # Give more time for target creation to propagate

    # Add permission for EventBridge to invoke Lambda
    echo "      Adding Lambda permission for EventBridge to invoke $lambda_function_name..."
    # Always try to remove and re-add permission to ensure it's clean and correct
    awslocal lambda remove-permission \
        --function-name "$lambda_function_name" \
        --statement-id "eventbridge-invoke-${rule_name_prefix}-${bucket_name//-/_}" \
        2>/dev/null || true # Ignore error if permission doesn't exist

    awslocal lambda add-permission \
        --function-name "$lambda_function_name" \
        --statement-id "eventbridge-invoke-${rule_name_prefix}-${bucket_name//-/_}" \
        --action lambda:InvokeFunction \
        --principal events.amazonaws.com \
        --source-arn "arn:aws:events:$region:000000000000:rule/$trigger_name" \
        || echo "      Error adding permission for $lambda_function_name from $trigger_name."
    sleep 3 # Small pause after adding permission

    echo "   Successfully set up trigger for $lambda_function_name from $bucket_name"
}

# --- Setup all triggers in sequence ---
echo "   Setting up triggers for the processing pipeline..."

# 1. raw-reviews-bucket -> File Splitter Lambda (NEW FIRST STAGE)
setup_s3_eventbridge_trigger "$RAW_BUCKET" "$FILE_SPLITTER_FUNCTION" "s3-raw-to-splitter"

# 2. split-reviews-bucket -> Preprocessing Lambda (UPDATED TRIGGER)
setup_s3_eventbridge_trigger "$SPLIT_BUCKET" "$PREPROCESSING_FUNCTION" "s3-split-to-preprocessing"

# 3. processed-reviews-bucket -> Profanity Check Lambda
setup_s3_eventbridge_trigger "$PROCESSED_BUCKET" "$PROFANITY_CHECK_FUNCTION" "s3-processed-to-profanity"

# 4. clean-reviews-bucket -> Sentiment Analysis Lambda
setup_s3_eventbridge_trigger "$CLEAN_BUCKET" "$SENTIMENT_ANALYSIS_FUNCTION" "s3-clean-to-sentiment"

# 5. flagged-reviews-bucket -> Sentiment Analysis Lambda (Optional)
setup_s3_eventbridge_trigger "$FLAGGED_BUCKET" "$SENTIMENT_ANALYSIS_FUNCTION" "s3-flagged-to-sentiment"
echo "   All pipeline triggers configured."

# --- Verification Step: List and Describe Rules ---
echo ""
echo "--- Verifying EventBridge Rules ---"
awslocal events list-rules --event-bus-name default
echo ""
awslocal events describe-rule --name "s3-raw-to-splitter-review-file-splitter-dev-trigger" 2>/dev/null || true
awslocal events describe-rule --name "s3-split-to-preprocessing-review-preprocessing-dev-trigger" 2>/dev/null || true
awslocal events describe-rule --name "s3-processed-to-profanity-review-profanity-check-dev-trigger" 2>/dev/null || true
awslocal events describe-rule --name "s3-clean-to-sentiment-review-sentiment-analysis-dev-trigger" 2>/dev/null || true
awslocal events describe-rule --name "s3-flagged-to-sentiment-review-sentiment-analysis-dev-trigger" 2>/dev/null || true
echo "--- EventBridge Rule Verification Complete ---"


echo ""
echo "==================================================="
echo " AWS Resources Setup Completed Successfully!     "
echo "==================================================="
echo ""
echo "Summary of Setup:"
echo "-----------------"
echo "- All S3 buckets created: $RAW_BUCKET, $SPLIT_BUCKET, $PROCESSED_BUCKET, $CLEAN_BUCKET, $FLAGGED_BUCKET, $FINAL_REVIEWS_BUCKET."
echo "- DynamoDB table created: $CUSTOMER_PROFANITY_TABLE_NAME."
echo "- All necessary SSM parameters set for buckets, DynamoDB, and ban threshold."
echo "- EventBridge triggers configured to chain Lambdas (now including file splitter)."
echo ""
echo "Important Next Step:"
echo "--------------------"
echo "1. **Deploy your Lambda Function Code to LocalStack.**"
echo "   (If you haven't already, run 'python3 package_lambda.py' to create zip files, then use 'awslocal lambda create-function' or 'update-function-code' for each.)"
echo "   Example deployment (repeat for each function, adjusting function name and zip file):"
echo "   awslocal lambda create-function --function-name $PREPROCESSING_FUNCTION --runtime python3.9 --zip-file fileb://deployments/preprocessing_deployment.zip --handler preprocessing.lambda_handler --role arn:aws:iam::000000000000:role/lambda-role --environment Variables={AWS_ENDPOINT_URL=http://host.docker.internal:4566} --timeout 30"
echo "   (Remember to adjust runtime, handler, and role ARN as per your setup. For LocalStack, a dummy ARN like 'arn:aws:iam::000000000000:role/lambda-role' usually suffices.)"
echo ""
echo "To Clean Up (Optional):"
echo "----------------------"
echo " - Stop LocalStack: 'localstack stop'"
echo " - Delete S3 buckets (be careful!): 'awslocal s3 rb s3://<bucket-name> --force'"
echo " - Delete DynamoDB table: 'awslocal dynamodb delete-table --table-name <table-name>'"
echo " - Delete EventBridge rules and targets (using 'remove-targets' then 'delete-rule')."
echo " - Delete SSM parameters: 'awslocal ssm delete-parameter --name <parameter-name>'"
