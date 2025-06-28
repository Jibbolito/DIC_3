import boto3
import json
import os
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
        upload_delay_seconds (float, optional): Delay in seconds between uploads to avoid throttling.
                                               Defaults to 0.1 seconds.
    """
    s3_client = boto3.client('s3', endpoint_url=aws_endpoint_url)

    processed_count = 0
    failed_count = 0

    print(f"Starting upload of reviews from '{file_path}' to bucket '{bucket_name}'...")

    try:
        with open(file_path, 'r') as f:
            batch = []
            batch_start_line = 1
            for line_num, line in enumerate(f, 1):
                try:
                    review_data = json.loads(line.strip())
                    batch.append(review_data)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON on line {line_num}: {e}. Skipping line: {line.strip()[:100]}...")
                    failed_count += 1
                    continue

                # When batch reaches 25 or it's the last line, upload
                if len(batch) == 25:
                    object_key = f"clean/batch_{batch_start_line}_{line_num}.jsonl"
                    try:
                        # Convert batch to JSONL (one JSON object per line)
                        jsonl_data = '\n'.join(json.dumps(obj) for obj in batch)
                        s3_client.put_object(
                            Bucket=bucket_name,
                            Key=object_key,
                            Body=jsonl_data,
                            ContentType='application/json'
                        )
                        processed_count += len(batch)
                        if processed_count % 1000 == 0:
                            print(f"  Uploaded {processed_count} reviews so far...")
                        if upload_delay_seconds > 0:
                            time.sleep(upload_delay_seconds)
                    except Exception as e:
                        print(f"Error uploading batch starting at line {batch_start_line}: {e}. Skipping batch.")
                        failed_count += len(batch)
                    batch = []
                    batch_start_line = line_num + 1

            # Upload any remaining reviews in the last batch
            if batch:
                object_key = f"clean/batch_{batch_start_line}_{line_num}.jsonl"
                try:
                    jsonl_data = '\n'.join(json.dumps(obj) for obj in batch)
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=object_key,
                        Body=jsonl_data,
                        ContentType='application/json'
                    )
                    processed_count += len(batch)
                except Exception as e:
                    print(f"Error uploading final batch starting at line {batch_start_line}: {e}. Skipping batch.")
                    failed_count += len(batch)

        print("\n--- Upload Summary ---")
        print(f"Total reviews processed for upload: {processed_count}")
        print(f"Total lines failed to upload: {failed_count}")
        print("Upload process completed.")

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    FILE_PATH = './data/reviews_devset.json'
    TARGET_BUCKET_NAME = 'raw-reviews-bucket' 
    AWS_LOCALSTACK_ENDPOINT = 'http://localhost:4566'
    UPLOAD_DELAY_SECONDS = 2

    os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)

    upload_reviews_to_s3(FILE_PATH, TARGET_BUCKET_NAME, 
                         aws_endpoint_url=AWS_LOCALSTACK_ENDPOINT, 
                         upload_delay_seconds=UPLOAD_DELAY_SECONDS)
