#!/bin/bash

# AWS Resources Setup Script
# This script automates:
# 1. AWS S3 bucket creation and SSM parameter registration
# 2. AWS DynamoDB table creation and SSM parameter registration (for profanity counts)
# 3. AWS EventBridge rule and target creation to chain Lambda functions via S3 events

set -e

echo "==================================================="
echo " Starting AWS Resources Setup                    "
echo "==================================================="
echo ""

# Check if LocalStack is running before proceeding
echo "📡 Checking LocalStack status..."
if ! curl -s http://localhost:4566/_localstack/health > /dev/null; then
    echo "   ERROR: LocalStack is not running. Please run ./setup_environment.sh first."
    exit 1
fi
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
PROCESSED_BUCKET="processed-reviews-bucket"
CLEAN_BUCKET="clean-reviews-bucket"
FLAGGED_BUCKET="flagged-reviews-bucket"
FINAL_REVIEWS_BUCKET="final-reviews-bucket"
CUSTOMER_PROFANITY_TABLE_NAME="CustomerProfanityCounts"
BAN_THRESHOLD_VALUE="3"

# Helper function to create S3 bucket
create_s3_bucket() {
    local bucket_name=$1
    echo "   Creating S3 bucket: $bucket_name..."
    aws --endpoint-url=http://localhost:4566 s3 mb "s3://$bucket_name" 2>/dev/null || echo "   Bucket $bucket_name already exists or could not be created."
}

# Create S3 buckets
create_s3_bucket "$RAW_BUCKET"
create_s3_bucket "$PROCESSED_BUCKET"
create_s3_bucket "$CLEAN_BUCKET"
create_s3_bucket "$FLAGGED_BUCKET"
create_s3_bucket "$FINAL_REVIEWS_BUCKET"
echo "   All S3 buckets checked/created."

# Create SSM Parameters for S3 bucket names
echo "   Creating SSM Parameters for S3 bucket names..."
aws --endpoint-url=http://localhost:4566 ssm put-parameter --name "/my-app/s3/raw_bucket_name" --type "String" --value "$RAW_BUCKET" --overwrite
aws --endpoint-url=http://localhost:4566 ssm put-parameter --name "/my-app/s3/processed_bucket_name" --type "String" --value "$PROCESSED_BUCKET" --overwrite
aws --endpoint-url=http://localhost:4566 ssm put-parameter --name "/my-app/s3/clean_bucket_name" --type "String" --value "$CLEAN_BUCKET" --overwrite
aws --endpoint-url=http://localhost:4566 ssm put-parameter --name "/my-app/s3/flagged_bucket_name" --type "String" --value "$FLAGGED_BUCKET" --overwrite
aws --endpoint-url=http://localhost:4566 ssm put-parameter --name "/my-app/s3/final_reviews_bucket_name" --type "String" --value "$FINAL_REVIEWS_BUCKET" --overwrite
echo "   SSM parameters for S3 buckets created."

# Create DynamoDB table
echo "   Creating DynamoDB table: $CUSTOMER_PROFANITY_TABLE_NAME..."
aws --endpoint-url=http://localhost:4566 dynamodb create-table \
    --table-name "$CUSTOMER_PROFANITY_TABLE_NAME" \
    --attribute-definitions \
        AttributeName=reviewer_id,AttributeType=S \
    --key-schema \
        AttributeName=reviewer_id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    2>/dev/null || echo "   DynamoDB table '$CUSTOMER_PROFANITY_TABLE_NAME' already exists or could not be created."
echo "   DynamoDB table '$CUSTOMER_PROFANITY_TABLE_NAME' checked/created."

# Create SSM Parameters for DynamoDB table and ban threshold
echo "   Creating SSM Parameters for DynamoDB table and ban threshold..."
aws --endpoint-url=http://localhost:4566 ssm put-parameter \
    --name "/my-app/dynamodb/customer_profanity_table_name" \
    --type "String" \
    --value "$CUSTOMER_PROFANITY_TABLE_NAME" \
    --overwrite
aws --endpoint-url=http://localhost:4566 ssm put-parameter \
    --name "/my-app/ban_threshold" \
    --type "String" \
    --value "$BAN_THRESHOLD_VALUE" \
    --overwrite
echo "   SSM parameters for DynamoDB and ban threshold created."

echo ""
echo "--- Section 2: EventBridge Setup for Lambda Triggers ---"

# Define Lambda function names (these must match your deployed Lambda function names in LocalStack)
PREPROCESSING_FUNCTION="review-preprocessing-dev"
PROFANITY_CHECK_FUNCTION="review-profanity-check-dev"
SENTIMENT_ANALYSIS_FUNCTION="review-sentiment-analysis-dev"

# Helper function to create S3-to-Lambda trigger via EventBridge
setup_s3_eventbridge_trigger() {
    local bucket_name=$1
    local lambda_function_name=$2
    local rule_name_prefix=$3
    local region=$AWS_DEFAULT_REGION # Use exported region

    echo "   Setting up trigger for $lambda_function_name on $bucket_name..."

    local trigger_name="${rule_name_prefix}-${lambda_function_name}-trigger"

    # Enable EventBridge for the S3 bucket if not already enabled
    echo "      Enabling EventBridge for bucket: $bucket_name..."
    aws --endpoint-url=http://localhost:4566 s3api put-bucket-notification-configuration \
        --bucket "$bucket_name" \
        --notification-configuration '{"EventBridgeConfiguration": {}}' \
        || true # Ignore error if already enabled

    # Create event pattern file
    local event_pattern_file="event-pattern-${lambda_function_name}.json"
    cat > "$event_pattern_file" << EOF
{
  "source": [
    "aws.s3"
  ],
  "detail-type": [
    "Object Created"
  ],
  "detail": {
    "bucket": {
      "name": [
        "${bucket_name}"
      ]
    }
  }
}
EOF

    # Create EventBridge rule
    echo "      Creating EventBridge rule: $trigger_name..."
    aws --endpoint-url=http://localhost:4566 events put-rule \
        --name "$trigger_name" \
        --event-pattern "file://$event_pattern_file" \
        --state ENABLED

    # Get function ARN
    echo "      Getting ARN for function: $lambda_function_name..."
    local function_arn
    if [[ -f "jq.exe" ]]; then
        function_arn=$(aws --endpoint-url=http://localhost:4566 lambda get-function --function-name "$lambda_function_name" | ./jq.exe -r .Configuration.FunctionArn)
    elif command -v jq &> /dev/null; then
        function_arn=$(aws --endpoint-url=http://localhost:4566 lambda get-function --function-name "$lambda_function_name" | jq -r .Configuration.FunctionArn)
    else
        echo "      jq not found, using manual parsing..."
        function_arn=$(aws --endpoint-url=http://localhost:4566 lambda get-function --function-name "$lambda_function_name" | grep -o '"FunctionArn":"[^"]*"' | cut -d'"' -f4)
    fi

    if [ "$function_arn" == "null" ] || [ -z "$function_arn" ]; then
        echo "   ERROR: Could not find function: $lambda_function_name. Please ensure it's deployed before setting up triggers."
        exit 1
    fi
    echo "      Found function ARN: $function_arn"

    # Add Lambda function as target
    echo "      Adding Lambda function as target for rule: $trigger_name..."
    aws --endpoint-url=http://localhost:4566 events put-targets \
        --rule "$trigger_name" \
        --targets Id="${lambda_function_name}-target",Arn="$function_arn"

    # Add permission for EventBridge to invoke Lambda
    echo "      Adding Lambda permission for EventBridge to invoke $lambda_function_name..."
    aws --endpoint-url=http://localhost:4566 lambda add-permission \
        --function-name "$lambda_function_name" \
        --statement-id "eventbridge-invoke-${rule_name_prefix}-${bucket_name//-/_}" \
        --action lambda:InvokeFunction \
        --principal events.amazonaws.com \
        --source-arn "arn:aws:events:$region:000000000000:rule/$trigger_name" \
        || true # Ignore error if permission already exists

    echo "   Successfully set up trigger for $lambda_function_name from $bucket_name"
    rm -f "$event_pattern_file"
}

# --- Setup all triggers in sequence ---
echo "   Setting up triggers for the processing pipeline..."
# 1. raw-reviews-bucket -> Preprocessing Lambda
setup_s3_eventbridge_trigger "$RAW_BUCKET" "$PREPROCESSING_FUNCTION" "s3-raw"

# 2. processed-reviews-bucket -> Profanity Check Lambda
setup_s3_eventbridge_trigger "$PROCESSED_BUCKET" "$PROFANITY_CHECK_FUNCTION" "s3-processed"

# 3. clean-reviews-bucket -> Sentiment Analysis Lambda
setup_s3_eventbridge_trigger "$CLEAN_BUCKET" "$SENTIMENT_ANALYSIS_FUNCTION" "s3-clean"

# 4. flagged-reviews-bucket -> Sentiment Analysis Lambda (Optional, if you want sentiment on flagged too)
setup_s3_eventbridge_trigger "$FLAGGED_BUCKET" "$SENTIMENT_ANALYSIS_FUNCTION" "s3-flagged"
echo "   All pipeline triggers configured."

echo ""
echo "--- Section 3: Testing Setup ---"

echo "   Uploading test files to $RAW_BUCKET to initiate the pipeline..."
# Test by uploading a clean and a profane file to S3 to trigger the chain
echo '{"asin": "TEST12345", "reviewerID": "TESTER1", "reviewText": "This is a great product! I really enjoy it. No bad words here.", "summary": "Awesome!", "overall": 5}' > test-review-clean.json
echo '{"asin": "TEST12346", "reviewerID": "TESTER2", "reviewText": "This product is a total scam and a rip-off. What a piece of garbage!", "summary": "Terrible!", "overall": 1}' > test-review-profane.json
echo '{"asin": "TEST12347", "reviewerID": "TESTER2", "reviewText": "Another awful product. This is truly worthless.", "summary": "Bad!", "overall": 2}' > test-review-profane-2.json
echo '{"asin": "TEST12348", "reviewerID": "TESTER2", "reviewText": "Just a bad experience, terrible.", "summary": "Ugh!", "overall": 1}' > test-review-profane-3.json
echo '{"asin": "TEST12349", "reviewerID": "TESTER2", "reviewText": "Seriously, this is a fraud. I am very angry.", "summary": "😡", "overall": 1}' > test-review-profane-4.json # Should trigger ban

aws --endpoint-url=http://localhost:4566 s3 cp test-review-clean.json s3://$RAW_BUCKET/test-review-clean.json
aws --endpoint-url=http://localhost:4566 s3 cp test-review-profane.json s3://$RAW_BUCKET/test-review-profane.json
aws --endpoint-url=http://localhost:4566 s3 cp test-review-profane-2.json s3://$RAW_BUCKET/test-review-profane-2.json
aws --endpoint-url=http://localhost:4566 s3 cp test-review-profane-3.json s3://$RAW_BUCKET/test-review-profane-3.json
aws --endpoint-url=http://localhost:4566 s3 cp test-review-profane-4.json s3://$RAW_BUCKET/test-review-profane-4.json

echo "   Test files uploaded to $RAW_BUCKET. The processing pipeline should now be active."
echo "   Monitor LocalStack logs for Lambda execution (e.g., 'localstack logs -f')."
echo "   Check S3 buckets ($PROCESSED_BUCKET, $CLEAN_BUCKET, $FLAGGED_BUCKET, $FINAL_REVIEWS_BUCKET) and DynamoDB table ($CUSTOMER_PROFANITY_TABLE_NAME) for results."

# Clean up local test files
sleep 5 # Give some time for initial triggers to fire
rm -f test-review-clean.json test-review-profane.json test-review-profane-2.json test-review-profane-3.json test-review-profane-4.json

echo ""
echo "==================================================="
echo " AWS Resources Setup Completed Successfully!     "
echo "==================================================="
echo ""
echo "Summary of Setup:"
echo "-----------------"
echo "- All S3 buckets created: $RAW_BUCKET, $PROCESSED_BUCKET, $CLEAN_BUCKET, $FLAGGED_BUCKET, $FINAL_REVIEWS_BUCKET."
echo "- DynamoDB table created: $CUSTOMER_PROFANITY_TABLE_NAME."
echo "- All necessary SSM parameters set for buckets, DynamoDB, and ban threshold."
echo "- EventBridge triggers configured to chain Lambdas."
echo ""
echo "Important Next Step:"
echo "--------------------"
echo "1. **Deploy your Lambda Function Code to LocalStack.**"
echo "   (If you haven't already, run 'python3 package_lambda.py' to create zip files, then use 'aws --endpoint-url=http://localhost:4566 lambda create-function' or 'update-function-code' for each.)"
echo "   Example deployment (repeat for each function, adjusting function name and zip file):"
echo "   aws --endpoint-url=http://localhost:4566 lambda create-function --function-name $PREPROCESSING_FUNCTION --runtime python3.9 --zip-file fileb://deployments/preprocessing_deployment.zip --handler preprocessing.lambda_handler --role arn:aws:iam::000000000000:role/lambda-role --environment Variables={AWS_ENDPOINT_URL=http://host.docker.internal:4566} --timeout 30"
echo "   (Remember to adjust runtime, handler, and role ARN as per your setup. For LocalStack, a dummy ARN like 'arn:aws:iam::000000000000:role/lambda-role' usually suffices.)"
echo ""
echo "To Clean Up (Optional):"
echo "----------------------"
echo " - Stop LocalStack: 'localstack stop'"
echo " - Delete S3 buckets (be careful!): 'aws --endpoint-url=http://localhost:4566 s3 rb s3://<bucket-name> --force'"
echo " - Delete DynamoDB table: 'aws --endpoint-url=http://localhost:4566 dynamodb delete-table --table-name <table-name>'"
echo " - Delete EventBridge rules and targets (using 'remove-targets' then 'delete-rule')."
echo " - Delete SSM parameters: 'aws --endpoint-url=http://localhost:4566 ssm delete-parameter --name <parameter-name>'"