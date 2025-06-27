import boto3
import json
import os

def generate_review_report():
    """
    Connects to LocalStack, retrieves review statistics from all S3 buckets
    and DynamoDB, and generates a consolidated JSON report.
    """
    # --- Configuration for LocalStack connection ---
    # Ensure these environment variables are set, or replace with direct values
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
    aws_default_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    aws_endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")

    # --- Define All Resource Names (from setup_eventbridge.sh and common naming) ---
    RAW_BUCKET = "raw-reviews-bucket"
    SPLIT_BUCKET = "split-reviews-bucket"
    PROCESSED_BUCKET = "processed-reviews-bucket"
    CLEAN_BUCKET = "clean-reviews-bucket"
    FLAGGED_BUCKET = "flagged-reviews-bucket"
    FINAL_REVIEWS_BUCKET = "final-reviews-bucket" # Also named 'analyzed-reviews-bucket' in some contexts, ensure consistency
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
        "s3_raw_bucket_count": 0,
        "s3_split_bucket_count": 0,
        "s3_processed_bucket_count": 0,
        "s3_clean_bucket_count": 0,
        "s3_flagged_bucket_count": 0,
        "s3_final_reviews_bucket_count": 0,
        "dynamodb_profanity_table_count": 0,
        "total_reviews_processed_to_final": 0, # Reviews that made it to the final S3 bucket
        "positive_reviews_final": 0,
        "neutral_reviews_final": 0,
        "negative_reviews_final": 0,
        "failed_profanity_check_flagged_bucket": 0, # From flagged bucket count
        "banned_users_count": 0,
        "banned_users": []
    }

    print("--- Generating Review Report ---")
    print(f"Connecting to LocalStack at: {aws_endpoint_url}")

    # Helper function to count objects in an S3 bucket
    def count_s3_objects(bucket_name):
        count = 0
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name)
            for page in pages:
                if 'Contents' in page:
                    count += len(page['Contents'])
            print(f"Counted {count} objects in '{bucket_name}'")
        except Exception as e:
            print(f"Error accessing S3 bucket '{bucket_name}': {e}")
        return count

    # --- 1. Get Ban Threshold from SSM ---
    ban_threshold = 3 # Default value
    try:
        response = ssm_client.get_parameter(Name=BAN_THRESHOLD_SSM_PARAM)
        ban_threshold = int(response['Parameter']['Value'])
        print(f"Retrieved ban threshold from SSM: {ban_threshold}")
    except Exception as e:
        print(f"Warning: Could not retrieve ban threshold from SSM ({e}). Using default: {ban_threshold}")

    # --- 2. Count Reviews in Each S3 Bucket ---
    report_data["s3_raw_bucket_count"] = count_s3_objects(RAW_BUCKET)
    report_data["s3_split_bucket_count"] = count_s3_objects(SPLIT_BUCKET)
    report_data["s3_processed_bucket_count"] = count_s3_objects(PROCESSED_BUCKET)
    report_data["s3_clean_bucket_count"] = count_s3_objects(CLEAN_BUCKET)
    report_data["s3_flagged_bucket_count"] = count_s3_objects(FLAGGED_BUCKET)
    report_data["s3_final_reviews_bucket_count"] = count_s3_objects(FINAL_REVIEWS_BUCKET)
    report_data["failed_profanity_check_flagged_bucket"] = report_data["s3_flagged_bucket_count"]


    # --- 3. Process Sentiment Counts (from Final Reviews Bucket) ---
    # This loop also doubles as the source for `total_reviews_processed_to_final`
    # print(f"Analyzing sentiment from '{FINAL_REVIEWS_BUCKET}' for detailed breakdown...")
    # try:
    #     paginator = s3_client.get_paginator('list_objects_v2')
    #     pages = paginator.paginate(Bucket=FINAL_REVIEWS_BUCKET)
    #     for page in pages:
    #         if 'Contents' in page:
    #             for obj in page['Contents']:
    #                 try:
    #                     # Get object content
    #                     response = s3_client.get_object(Bucket=FINAL_REVIEWS_BUCKET, Key=obj['Key'])
    #                     review_content = response['Body'].read().decode('utf-8')
    #                     review_json = json.loads(review_content)

    #                     # Assuming your sentiment analysis Lambda adds a 'sentiment_analysis' field
    #                     sentiment_label = review_json.get('sentiment_analysis', {}).get('sentiment_label')
                        
    #                     if sentiment_label == 'positive':
    #                         report_data["positive_reviews_final"] += 1
    #                     elif sentiment_label == 'neutral':
    #                         report_data["neutral_reviews_final"] += 1
    #                     elif sentiment_label == 'negative':
    #                         report_data["negative_reviews_final"] += 1
    #                     # If sentiment is missing or unexpected, it won't be counted in these categories
    #                     # but still contributes to total_reviews_processed_to_final.

    #                 except json.JSONDecodeError:
    #                     print(f"Warning: Could not decode JSON for {obj['Key']} in {FINAL_REVIEWS_BUCKET}. Skipping detailed sentiment analysis for this file.")
    #                 except Exception as e:
    #                     print(f"Warning: Error processing {obj['Key']} in {FINAL_REVIEWS_BUCKET}: {e}")
    #     # The total count for final reviews bucket is already in s3_final_reviews_bucket_count
    #     report_data["total_reviews_processed_to_final"] = report_data["s3_final_reviews_bucket_count"]
    #     print(f"Detailed sentiment analysis completed for {report_data['total_reviews_processed_to_final']} reviews from '{FINAL_REVIEWS_BUCKET}'")
    # except Exception as e:
    #     print(f"Error accessing '{FINAL_REVIEWS_BUCKET}' for detailed sentiment analysis: {e}")

    # --- 4. Get Banned Users and Total DynamoDB Items ---
    print(f"Scanning DynamoDB table '{CUSTOMER_PROFANITY_TABLE_NAME}'...")
    try:
        response = dynamodb_client.scan(TableName=CUSTOMER_PROFANITY_TABLE_NAME)
        report_data["dynamodb_profanity_table_count"] = response.get('Count', 0)

        for item in response.get('Items', []):
            reviewer_id = item.get('reviewer_id', {}).get('S')
            profanity_count_str = item.get('profanity_count', {}).get('N', '0') # Get as string
            
            # Convert to int, handle potential errors if 'N' isn't clean
            try:
                profanity_count = int(profanity_count_str)
            except ValueError:
                profanity_count = 0 # Default if conversion fails

            # Check if 'is_banned' attribute exists and is True (S for string 'True' or B for boolean type)
            is_banned_val = item.get('is_banned', {}).get('BOOL', False) # Assuming BOOL type for boolean
            # Fallback if 'is_banned' is stored as String 'true' or 'false'
            if not is_banned_val and item.get('is_banned', {}).get('S', '').lower() == 'true':
                 is_banned_val = True

            if is_banned_val: # Only count if explicitly marked as banned
                report_data["banned_users"].append({
                    "user_id": reviewer_id,
                    "profanity_count": profanity_count
                })
        report_data["banned_users_count"] = len(report_data["banned_users"])
        print(f"Found {report_data['banned_users_count']} banned users from {report_data['dynamodb_profanity_table_count']} total entries.")

    except Exception as e:
        print(f"Error accessing DynamoDB table '{CUSTOMER_PROFANITY_TABLE_NAME}': {e}")

    # --- Save the report to a JSON file ---
    output_filename = "review_pipeline_report.json"
    with open(output_filename, 'w') as f:
        json.dump(report_data, f, indent=2)

    print(f"\nReport generated successfully and saved to '{output_filename}'")
    print("\n--- Report Summary ---")
    for key, value in report_data.items():
        if key != "banned_users": # Print banned_users separately
            print(f"- {key.replace('_', ' ').replace('s3', 'S3').replace('dynamodb', 'DynamoDB').title()}: {value}")
    if report_data["banned_users"]:
        print("- Banned Users:")
        for user in report_data["banned_users"]:
            print(f"  - User ID: {user['user_id']}, Profanity Count: {user['profanity_count']}")


if __name__ == "__main__":
    generate_review_report()
