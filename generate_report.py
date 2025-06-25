import boto3
import json
import os

def generate_review_report():
    """
    Connects to LocalStack, retrieves review statistics from S3 buckets
    and DynamoDB, and generates a consolidated JSON report.
    """
    # --- Configuration for LocalStack connection ---
    # Ensure these environment variables are set, or replace with direct values
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
    aws_default_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    aws_endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")

    # --- Define Resource Names (from setup_eventbridge.sh) ---
    FINAL_REVIEWS_BUCKET = "final-reviews-bucket"
    FLAGGED_BUCKET = "flagged-reviews-bucket"
    CUSTOMER_PROFANITY_TABLE_NAME = "CustomerProfanityCounts"
    BAN_THRESHOLD_SSM_PARAM = "/my-app/ban_threshold"

    # --- Initialize AWS clients for LocalStack ---
    s3_client = boto3.client(
        "s3",
        endpoint_url=aws_endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_default_region,
    )
    dynamodb_client = boto3.client(
        "dynamodb",
        endpoint_url=aws_endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_default_region,
    )
    ssm_client = boto3.client(
        "ssm",
        endpoint_url=aws_endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_default_region,
    )

    report_data = {
        "total_reviews": 0,
        "positive_reviews": 0,
        "neutral_reviews": 0,
        "negative_reviews": 0,
        "failed_profanity_check": 0,
        "banned_users_count": 0,
        "banned_users": []
    }

    print("--- Generating Review Report ---")
    print(f"Connecting to LocalStack at: {aws_endpoint_url}")

    # --- 1. Get Ban Threshold from SSM ---
    ban_threshold = 3 # Default value
    try:
        response = ssm_client.get_parameter(Name=BAN_THRESHOLD_SSM_PARAM)
        ban_threshold = int(response['Parameter']['Value'])
        print(f"Retrieved ban threshold from SSM: {ban_threshold}")
    except Exception as e:
        print(f"Warning: Could not retrieve ban threshold from SSM ({e}). Using default: {ban_threshold}")

    # --- 2. Count Failed Profanity Checks (from Flagged Bucket) ---
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=FLAGGED_BUCKET)
        for page in pages:
            if 'Contents' in page:
                report_data["failed_profanity_check"] += len(page['Contents'])
        print(f"Counted {report_data['failed_profanity_check']} reviews in '{FLAGGED_BUCKET}'")
    except Exception as e:
        print(f"Error accessing '{FLAGGED_BUCKET}': {e}")

    # --- 3. Process Sentiment Counts (from Final Reviews Bucket) ---
    print(f"Processing sentiment from '{FINAL_REVIEWS_BUCKET}'...")
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=FINAL_REVIEWS_BUCKET)
        for page in pages:
            if 'Contents' in page:
                report_data["total_reviews"] += len(page['Contents'])
                for obj in page['Contents']:
                    try:
                        # Get object content
                        response = s3_client.get_object(Bucket=FINAL_REVIEWS_BUCKET, Key=obj['Key'])
                        review_content = response['Body'].read().decode('utf-8')
                        review_json = json.loads(review_content)

                        # Assuming your sentiment analysis Lambda adds a 'sentiment' field
                        sentiment = review_json.get('sentiment')
                        if sentiment == 'POSITIVE':
                            report_data["positive_reviews"] += 1
                        elif sentiment == 'NEUTRAL':
                            report_data["neutral_reviews"] += 1
                        elif sentiment == 'NEGATIVE':
                            report_data["negative_reviews"] += 1
                        # If sentiment is missing or unexpected, it won't be counted in these categories
                        # but still contributes to total_reviews.

                    except json.JSONDecodeError:
                        print(f"Warning: Could not decode JSON for {obj['Key']}. Skipping sentiment analysis for this file.")
                    except Exception as e:
                        print(f"Warning: Error processing {obj['Key']}: {e}")
        print(f"Processed {report_data['total_reviews']} reviews from '{FINAL_REVIEWS_BUCKET}'")
    except Exception as e:
        print(f"Error accessing '{FINAL_REVIEWS_BUCKET}': {e}")

    # --- 4. Get Banned Users (from DynamoDB) ---
    print(f"Scanning DynamoDB table '{CUSTOMER_PROFANITY_TABLE_NAME}' for banned users...")
    try:
        response = dynamodb_client.scan(TableName=CUSTOMER_PROFANITY_TABLE_NAME)
        for item in response.get('Items', []):
            reviewer_id = item.get('reviewer_id', {}).get('S')
            unpolite_count = int(item.get('unpolite_count', {}).get('N', 0)) # Ensure 'N' for Number type

            if reviewer_id and unpolite_count >= ban_threshold:
                report_data["banned_users"].append({
                    "user_id": reviewer_id,
                    "unpolite_count": unpolite_count
                })
        report_data["banned_users_count"] = len(report_data["banned_users"])
        print(f"Found {report_data['banned_users_count']} banned users.")

    except Exception as e:
        print(f"Error accessing DynamoDB table '{CUSTOMER_PROFANITY_TABLE_NAME}': {e}")

    # --- Save the report to a JSON file ---
    output_filename = "review_pipeline_report.json"
    with open(output_filename, 'w') as f:
        json.dump(report_data, f, indent=2)

    print(f"\nReport generated successfully and saved to '{output_filename}'")
    print("\n--- Report Summary ---")
    for key, value in report_data.items():
        if key != "banned_users": # Print banned_users separately if needed
            print(f"- {key.replace('_', ' ').title()}: {value}")
    if report_data["banned_users"]:
        print("- Banned Users:")
        for user in report_data["banned_users"]:
            print(f"  - User ID: {user['user_id']}, Unpolite Count: {user['unpolite_count']}")


if __name__ == "__main__":
    generate_review_report()
