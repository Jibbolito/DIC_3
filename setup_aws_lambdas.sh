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

    # Delete existing functions first to ensure a clean deployment
    echo "Cleaning up existing functions (if any)..."
    # Using 2>/dev/null || true to suppress "function not found" errors if they don't exist
    awslocal lambda delete-function --function-name review-preprocessing-dev --endpoint-url="$AWS_ENDPOINT_URL" 2>/dev/null || true
    awslocal lambda delete-function --function-name review-profanity-check-dev --endpoint-url="$AWS_ENDPOINT_URL" 2>/dev/null || true
    awslocal lambda delete-function --function-name review-sentiment-analysis-dev --endpoint-url="$AWS_ENDPOINT_URL" 2>/dev/null || true

    echo "Creating preprocessing function..."
    awslocal lambda create-function \
        --function-name review-preprocessing-dev \
        --runtime python3.10 \
        --zip-file fileb://deployments/preprocessing_deployment.zip \
        --handler lambda_function.lambda_handler \
        --role arn:aws:iam::000000000000:role/lambda-role \
        --timeout 600 \
        --memory-size 4096 \
        --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566}" \
        --endpoint-url="$AWS_ENDPOINT_URL"

    echo "Creating profanity check function..."
    awslocal lambda create-function \
        --function-name review-profanity-check-dev \
        --runtime python3.10 \
        --zip-file fileb://deployments/profanity_check_deployment.zip \
        --handler lambda_function.lambda_handler \
        --role arn:aws:iam::000000000000:role/lambda-role \
        --timeout 600 \
        --memory-size 4096 \
        --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566}" \
        --endpoint-url="$AWS_ENDPOINT_URL"

    echo "Creating sentiment analysis function..."
    awslocal lambda create-function \
        --function-name review-sentiment-analysis-dev \
        --runtime python3.10 \
        --zip-file fileb://deployments/sentiment_analysis_deployment.zip \
        --handler lambda_function.lambda_handler \
        --role arn:aws:iam::000000000000:role/lambda-role \
        --timeout 600 \
        --memory-size 4096 \
        --environment Variables="{AWS_ENDPOINT_URL=http://host.docker.internal:4566}" \
        --endpoint-url="$AWS_ENDPOINT_URL"

    echo "==================================================="
    echo " Lambda Functions Deployment Completed!          "
    ===================================================
    echo ""
    