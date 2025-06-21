#!/bin/bash

# EventBridge Setup Script for Multiple Lambda Triggers
# This script implements the tips & tricks recommendation for triggering multiple functions

set -e

echo "   Setting up EventBridge for multiple Lambda triggers..."

# Configuration
RAW_BUCKET="raw-reviews-bucket"
PREPROCESSING_FUNCTION="review-preprocessing-dev"
TRIGGER_NAME="review-processing-trigger"
REGION="us-east-1"

# Check if LocalStack is running
echo "📡 Checking LocalStack status..."
if ! curl -s http://localhost:4566/_localstack/health > /dev/null; then
    echo "   LocalStack is not running. Please start LocalStack first:"
    echo "   localstack start"
    exit 1
fi

echo "   LocalStack is running"

# Set AWS CLI to use LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=$REGION
export AWS_ENDPOINT_URL=http://localhost:4566

echo "🔧 Step 1: Enable EventBridge for S3 bucket..."

# Enable EventBridge for the S3 bucket
awslocal s3api put-bucket-notification-configuration \
    --bucket $RAW_BUCKET \
    --notification-configuration '{"EventBridgeConfiguration": {}}'

echo "   EventBridge enabled for bucket: $RAW_BUCKET"

echo "   Step 2: Creating event pattern file..."

# Create event pattern file
cat > event-pattern.json << 'EOF'
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
        "raw-reviews-bucket"
      ]
    }
  }
}
EOF

echo "   Event pattern file created"

echo "   Step 3: Creating EventBridge rule..."

# Create EventBridge rule
awslocal events put-rule \
    --name $TRIGGER_NAME \
    --event-pattern file://event-pattern.json \
    --state ENABLED

echo "   EventBridge rule created: $TRIGGER_NAME"

echo "   Step 4: Adding Lambda function as target..."

# Get function ARN
FUNCTION_ARN=$(awslocal lambda get-function --function-name $PREPROCESSING_FUNCTION | jq -r .Configuration.FunctionArn)

if [ "$FUNCTION_ARN" == "null" ] || [ -z "$FUNCTION_ARN" ]; then
    echo "   Could not find function: $PREPROCESSING_FUNCTION"
    echo "   Please ensure the function is deployed first"
    exit 1
fi

echo "   Found function ARN: $FUNCTION_ARN"

# Add Lambda function as target
awslocal events put-targets \
    --rule $TRIGGER_NAME \
    --targets Id=preprocessing-target,Arn=$FUNCTION_ARN

echo "   Lambda function added as target"

echo "   Step 5: Adding Lambda permission for EventBridge..."

# Add permission for EventBridge to invoke Lambda
awslocal lambda add-permission \
    --function-name $PREPROCESSING_FUNCTION \
    --statement-id eventbridge-invoke \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn "arn:aws:events:$REGION:000000000000:rule/$TRIGGER_NAME"

echo "   Lambda permission added"

echo "   Step 6: Testing the setup..."

# Test by uploading a file to S3
echo '{"test": "EventBridge trigger test"}' > test-eventbridge.json

awslocal s3 cp test-eventbridge.json s3://$RAW_BUCKET/test-eventbridge.json

echo "   Test file uploaded"

# Wait a moment for processing
sleep 2

# Check if function was triggered (check logs if available)
echo "   Checking function invocations..."
awslocal logs describe-log-groups --log-group-name-prefix "/aws/lambda/$PREPROCESSING_FUNCTION" || echo "    No logs available yet"

# Clean up test file
awslocal s3 rm s3://$RAW_BUCKET/test-eventbridge.json
rm -f test-eventbridge.json event-pattern.json

echo ""
echo "   EventBridge setup completed successfully!"
echo ""
echo "   Configuration Summary:"
echo "   Bucket: $RAW_BUCKET"
echo "   Rule: $TRIGGER_NAME"
echo "   Target: $PREPROCESSING_FUNCTION"
echo ""
echo "   Next steps:"
echo "   1. Upload review files to s3://$RAW_BUCKET"
echo "   2. Monitor Lambda execution in CloudWatch logs"
echo "   3. Add additional functions as targets if needed:"
echo "      awslocal events put-targets --rule $TRIGGER_NAME --targets Id=new-target,Arn=NEW_FUNCTION_ARN"
echo ""
echo "   To cleanup EventBridge setup:"
echo "   awslocal events remove-targets --rule $TRIGGER_NAME --ids preprocessing-target"
echo "   awslocal events delete-rule --name $TRIGGER_NAME"