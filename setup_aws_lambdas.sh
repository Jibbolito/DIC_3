echo "==================================================="
echo " Lambda Functions Deployment Started!            "
echo "==================================================="
echo ""

# Set AWS CLI to use LocalStack (important for these commands)
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION="us-east-1"
export AWS_ENDPOINT_URL="http://localhost:4566"

# Delete existing functions and layers first
echo "Cleaning up existing functions and layers..."
aws lambda delete-function --function-name review-preprocessing-dev --endpoint-url=http://localhost:4566 2>/dev/null || true
aws lambda delete-function --function-name review-profanity-check-dev --endpoint-url=http://localhost:4566 2>/dev/null || true
aws lambda delete-function --function-name review-sentiment-analysis-dev --endpoint-url=http://localhost:4566 2>/dev/null || true
aws lambda delete-layer-version --layer-name nltk-layer --version-number 1 --endpoint-url=http://localhost:4566 2>/dev/null || true

echo "Creating NLTK Lambda layer..."
LAYER_VERSION=$(aws lambda publish-layer-version \
    --layer-name nltk-layer \
    --zip-file fileb://deployments/nltk_layer.zip \
    --compatible-runtimes python3.10 \
    --endpoint-url=http://localhost:4566 \
    --query 'Version' --output text)

echo "NLTK layer created with version: $LAYER_VERSION"

echo "Creating preprocessing function..."
aws lambda create-function \
    --function-name review-preprocessing-dev \
    --runtime python3.10 \
    --zip-file fileb://deployments/preprocessing_deployment.zip \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 300 \
    --memory-size 512 \
    --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566}" \
    --endpoint-url=http://localhost:4566

echo "Creating profanity check function..."
aws lambda create-function \
    --function-name review-profanity-check-dev \
    --runtime python3.10 \
    --zip-file fileb://deployments/profanity_check_deployment.zip \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 300 \
    --memory-size 256 \
    --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566}" \
    --endpoint-url=http://localhost:4566

echo "Creating sentiment analysis function..."
aws lambda create-function \
    --function-name review-sentiment-analysis-dev \
    --runtime python3.10 \
    --zip-file fileb://deployments/sentiment_analysis_deployment.zip \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --timeout 300 \
    --memory-size 512 \
    --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566}" \
    --endpoint-url=http://localhost:4566

echo "==================================================="
echo " Lambda Functions Deployment Completed!          "
echo "==================================================="
echo ""