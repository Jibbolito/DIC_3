#!/bin/bash

# Usage: ./run_on_reviews_devset.sh <file_to_upload>
FILE_TO_UPLOAD="$1"

if [ -z "$FILE_TO_UPLOAD" ]; then
    echo "Usage: $0 <file_to_upload>"
    exit 1
fi

# Ensure these are set in your current terminal session
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION="us-east-1"
export AWS_ENDPOINT_URL="http://localhost:4566"

# Upload the file
# aws s3 cp data/reviews_devset.json s3://raw-reviews-bucket/my_new_review.json --endpoint-url="$AWS_ENDPOINT_URL"
aws s3 cp ./data/batches/"$FILE_TO_UPLOAD" s3://raw-reviews-bucket/my_new_review.json --endpoint-url="$AWS_ENDPOINT_URL"