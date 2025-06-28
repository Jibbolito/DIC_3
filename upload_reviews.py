import boto3
import json
import os
import uuid
import time


def upload_reviews_to_s3(file_path, bucket_name, aws_endpoint_url=None, upload_delay_seconds=0.1):
    """
    Reads a file line by line, assuming each line is a JSON object (review),
    and uploads each review as a separate S3 object to the specified bucket.

    Args:
        file_path (str): The path to the input file (e.g., './data/reviews_devset.json').
        bucket_name (str): The name of the S3 bucket to upload to (e.g., 'clean-reviews-bucket').
        aws_endpoint_url (str, optional): The AWS endpoint URL for LocalStack.
                                          Defaults to None (uses default AWS endpoint).
    """
    s3_client = boto3.client('s3', endpoint_url=aws_endpoint_url)

    processed_count = 0
    failed_count = 0

    print(f"Starting upload of reviews from '{file_path}' to bucket '{bucket_name}'...")

    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    review_data = json.loads(line.strip())

                    # Use 'review_id' if available, otherwise generate a unique ID
                    # or use 'asin' (Amazon Standard Identification Number) and 'reviewerID' for uniqueness
                    if 'review_id' in review_data:
                        object_key = f"clean/{review_data['review_id']}.json"
                    elif 'asin' in review_data and 'reviewerID' in review_data:
                        # Create a more robust key using both ASIN and reviewerID
                        object_key = f"clean/{review_data['asin']}_{review_data['reviewerID']}_{uuid.uuid4()}.json"
                    else:
                        object_key = f"clean/review_{uuid.uuid4()}.json" # Fallback to a truly unique ID

                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=object_key,
                        Body=json.dumps(review_data),
                        ContentType='application/json'
                    )
                    processed_count += 1
                    if processed_count % 1000 == 0:
                        print(f"  Uploaded {processed_count} reviews so far...")
                    
                    if upload_delay_seconds > 0:
                        time.sleep(upload_delay_seconds)

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_num}: {e}. Skipping line: {line.strip()[:100]}...")
                    failed_count += 1
                except Exception as e:
                    print(f"Error uploading object for line {line_num}: {e}. Skipping line.")
                    failed_count += 1

        print("\n--- Upload Summary ---")
        print(f"Total reviews processed for upload: {processed_count}")
        print(f"Total lines failed to upload: {failed_count}")
        print("Upload process completed.")

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # Define your file path and S3 bucket name
    FILE_PATH = './data/reviews_devset.json'
    TARGET_BUCKET_NAME = 'raw-reviews-bucket' 

    # --- Configuration for LocalStack ---
    # If you are running LocalStack, uncomment the line below and ensure your LocalStack is running
    # If you are using real AWS, keep it commented out.
    AWS_LOCALSTACK_ENDPOINT = 'http://localhost:4566'

    UPLOAD_DELAY_SECONDS = 0.5

    # Ensure the 'data' directory exists
    os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)

    # Call the function to start the upload
    upload_reviews_to_s3(FILE_PATH, TARGET_BUCKET_NAME, 
                         aws_endpoint_url=AWS_LOCALSTACK_ENDPOINT, 
                         upload_delay_seconds=UPLOAD_DELAY_SECONDS)
