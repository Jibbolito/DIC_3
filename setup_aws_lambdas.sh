echo "==================================================="
echo " Lambda Functions Creation Started!              "
echo "==================================================="
echo ""

# Set AWS CLI to use LocalStack (important for these commands)
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION="us-east-1"
export AWS_ENDPOINT_URL="http://localhost:4566"

# --- Deploy Preprocessing Lambda Function ---
# Function Name: review-preprocessing-dev
# Handler: lambda_function.lambda_handler (since your files are lambda_function.py)
# Zip File: deployments/preprocessing_deployment.zip
aws lambda create-function \
    --function-name review-preprocessing-dev \
    --runtime python3.12 \
    --zip-file fileb://deployments/preprocessing_deployment.zip \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566}" \
    --memory-size 1024 \
    --timeout 30 \
    --endpoint-url=http://localhost:4566

# --- Deploy Profanity Check Lambda Function ---
# Function Name: review-profanity-check-dev
# Handler: lambda_function.lambda_handler
# Zip File: deployments/profanity_check_deployment.zip
aws lambda create-function \
    --function-name review-profanity-check-dev \
    --runtime python3.12 \
    --zip-file fileb://deployments/profanity_check_deployment.zip \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566}" \
    --memory-size 1024 \
    --timeout 30 \
    --endpoint-url=http://localhost:4566

# --- Deploy Sentiment Analysis Lambda Function ---
# Function Name: review-sentiment-analysis-dev
# Handler: lambda_function.lambda_handler
# Zip File: deployments/sentiment_analysis_deployment.zip
aws lambda create-function \
    --function-name review-sentiment-analysis-dev \
    --runtime python3.12 \
    --zip-file fileb://deployments/sentiment_analysis_deployment.zip \
    --handler lambda_function.lambda_handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566}" \
    --memory-size 1024 \
    --timeout 30 \
    --endpoint-url=http://localhost:4566

echo "==================================================="
echo " Lambda Functions Creation Completed!            "
echo "==================================================="
echo ""