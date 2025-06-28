import json
import boto3
import logging
import os
import traceback # Import traceback
import uuid # Required for generating UUIDs if review_id is missing
from typing import Dict, Any
from profanityfilter import ProfanityFilter

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
# Initialize DynamoDB client, use endpoint_url for LocalStack
dynamodb = boto3.resource('dynamodb', endpoint_url=os.environ.get('AWS_ENDPOINT_URL'))
ssm_client = boto3.client('ssm', endpoint_url=os.environ.get('AWS_ENDPOINT_URL'))

pf = ProfanityFilter()

# Configuration variables, initialized to None and loaded once
FLAGGED_BUCKET = None
CLEAN_BUCKET = None
CUSTOMER_PROFANITY_TABLE_NAME = None
BAN_THRESHOLD = None
customer_profanity_table = None
_config_loaded = False

def load_config():
    """Loads configuration from SSM Parameter Store."""
    global FLAGGED_BUCKET, CLEAN_BUCKET, CUSTOMER_PROFANITY_TABLE_NAME, BAN_THRESHOLD, customer_profanity_table, _config_loaded
    if _config_loaded:
        return

    try:
        FLAGGED_BUCKET = ssm_client.get_parameter(Name='/my-app/s3/flagged_bucket_name', WithDecryption=True)['Parameter']['Value']
        CLEAN_BUCKET = ssm_client.get_parameter(Name='/my-app/s3/clean_bucket_name', WithDecryption=True)['Parameter']['Value']
        CUSTOMER_PROFANITY_TABLE_NAME = ssm_client.get_parameter(Name='/my-app/dynamodb/customer_profanity_table_name', WithDecryption=True)['Parameter']['Value']
        BAN_THRESHOLD = int(ssm_client.get_parameter(Name='/my-app/ban_threshold', WithDecryption=True)['Parameter']['Value'])
        
        customer_profanity_table = dynamodb.Table(CUSTOMER_PROFANITY_TABLE_NAME)
        _config_loaded = True
        logger.info("Configuration loaded from SSM.")
    except ssm_client.exceptions.ParameterNotFound as e:
        logger.error(f"Required SSM parameter not found: {e}. Falling back to defaults.")
        # Fallback for local testing/dev if SSM not setup
        FLAGGED_BUCKET = 'flagged-reviews-bucket'
        CLEAN_BUCKET = 'clean-reviews-bucket'
        CUSTOMER_PROFANITY_TABLE_NAME = 'CustomerProfanityCounts'
        BAN_THRESHOLD = 3 # Default ban threshold set to 3 for explicit ban on 3rd review
        customer_profanity_table = dynamodb.Table(CUSTOMER_PROFANITY_TABLE_NAME)
        _config_loaded = True
    except Exception as e:
        logger.critical(f"Failed to load SSM parameters at initialization: {e}. Lambda cannot proceed. Stack trace: {traceback.format_exc()}")
        raise # Re-raise if critical configuration cannot be loaded

def check_profanity_in_text(text: str) -> Dict:
    """
    Check for profanity in text using the profanityfilter package
    
    Args:
        text (str): Text to analyze
        
    Returns:
        dict: Analysis results including profanity status and details
    """
    if not text or not isinstance(text, str):
        return {
            'contains_profanity': False,
            'censored_text': ''
        }
    
    # Use profanityfilter to detect profanity
    contains_profanity = pf.is_profane(text)
    censored_text = pf.censor(text)
    
    return {
        'contains_profanity': contains_profanity,
        'censored_text': censored_text
    }

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    global _config_loaded # Ensure _config_loaded is updated correctly if needed

    try:
        if not _config_loaded:
            load_config() # Load config on first invocation (or cold start)

        logger.info(f"Profanity check Lambda triggered with event: {json.dumps(event)}")

        # Get bucket and object information from S3 event
        try:
            bucket_name = event['Records'][0]['s3']['bucket']['name']
            object_key = event['Records'][0]['s3']['object']['key']
        except (KeyError, IndexError) as e:
            logger.error(f"Invalid S3 event structure: {e}. Event: {json.dumps(event)}")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid S3 event structure'})
            }

        logger.info(f"Processing file: {object_key} from bucket: {bucket_name}")

        # Download the processed review from S3
        review_data = {}
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            review_data = json.loads(response['Body'].read().decode('utf-8'))
        except s3_client.exceptions.NoSuchKey:
            logger.warning(f"Object {object_key} not found in bucket {bucket_name}. Skipping.")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': f"Object {object_key} not found, skipped."})
            }
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON from S3 object {object_key}: {e}. Content might be malformed. Stack trace: {traceback.format_exc()}")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f"Malformed JSON in {object_key}"})
            }
        except Exception as e:
            logger.error(f"Error downloading or parsing S3 object {object_key}: {e}. Stack trace: {traceback.format_exc()}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Failed to retrieve or parse review data from S3'})
            }

        # Validate reviewer_id and review_id early
        reviewer_id = review_data.get('reviewer_id')
        review_id = review_data.get('review_id')

        if not reviewer_id:
            logger.warning(f"Missing 'reviewer_id' in review data for object {object_key}. Skipping processing for this review.")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing reviewer_id in review data'})
            }
        if not review_id:
            logger.warning(f"Missing 'review_id' in review data for object {object_key}. Generating a UUID for S3 key.")
            review_id = str(uuid.uuid4()) # Generating UUID for review_id if missing

        # Perform profanity check on processed text fields
        summary_check = check_profanity_in_text(review_data.get('processed_summary', ''))
        reviewtext_check = check_profanity_in_text(review_data.get('processed_reviewText', ''))
        overall_check = check_profanity_in_text(review_data.get('processed_overall', ''))

        contains_profanity = (
            summary_check['contains_profanity'] or
            reviewtext_check['contains_profanity'] or
            overall_check['contains_profanity']
        )

        # Update review data with profanity analysis
        review_data.update({
            'processing_stage': 'profanity_checked',
            'profanity_analysis': {
                'contains_profanity': contains_profanity,
                'summary_profanity': summary_check,
                'reviewtext_profanity': reviewtext_check,
                'overall_profanity': overall_check
            }
        })

        # --- Profanity Review Count and Banning Logic ---
        is_banned = False
        current_profanity_review_count = 0

        if contains_profanity:
            try:
                # Increment profanity review count in DynamoDB
                response = customer_profanity_table.update_item(
                    Key={'reviewer_id': reviewer_id},
                    UpdateExpression='SET profanity_review_count = if_not_exists(profanity_review_count, :start) + :inc',
                    ExpressionAttributeValues={
                        ':start': 0,
                        ':inc': 1
                    },
                    ReturnValues='UPDATED_NEW'
                )
                current_profanity_review_count = int(response['Attributes'].get('profanity_review_count', 0))
                logger.info(f"Reviewer '{reviewer_id}' profanity review count updated to: {current_profanity_review_count}")

                # Check for banning: Ban happens when the profanity review count is
                # equal to or exceeds the BAN_THRESHOLD.
                if current_profanity_review_count >= BAN_THRESHOLD:
                    customer_profanity_table.update_item(
                        Key={'reviewer_id': reviewer_id},
                        UpdateExpression='SET is_banned = :val',
                        ExpressionAttributeValues={
                            ':val': True
                        }
                    )
                    is_banned = True
                    logger.warning(f"Reviewer '{reviewer_id}' has been banned due to excessive profane reviews ({current_profanity_review_count}).")
            except dynamodb.meta.client.exceptions.ProvisionedThroughputExceededException as ddb_e:
                 logger.error(f"DynamoDB throughput exceeded for reviewer '{reviewer_id}': {str(ddb_e)}. Stack trace: {traceback.format_exc()}")
                 # In a production scenario, consider implementing retry logic or sending to a DLQ here.
            except Exception as ddb_e:
                logger.error(f"Unexpected error updating DynamoDB for reviewer '{reviewer_id}': {str(ddb_e)}. Stack trace: {traceback.format_exc()}")
                # Decide if this should halt processing or just log. For now, continuing.

        review_data['profanity_analysis']['reviewer_banned'] = is_banned
        # The profanity_review_count in the output payload reflects the count *after* a profane review.
        # If the current review is clean, it will be 0 as no update occurred for this review.
        review_data['profanity_analysis']['current_reviewer_profanity_review_count'] = current_profanity_review_count if contains_profanity else 0

        # Determine next bucket based on profanity status
        next_bucket = FLAGGED_BUCKET if contains_profanity else CLEAN_BUCKET
        next_key = f"{'flagged' if contains_profanity else 'clean'}/{review_id}.json"

        # Save analyzed review to appropriate S3 bucket
        try:
            s3_client.put_object(
                Bucket=next_bucket,
                Key=next_key,
                Body=json.dumps(review_data, indent=2),
                ContentType='application/json'
            )
        except Exception as s3_put_e:
            logger.error(f"Error saving processed review to S3 ({next_bucket}/{next_key}): {s3_put_e}. Stack trace: {traceback.format_exc()}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Failed to save processed review to S3'})
            }

        logger.info(f"Profanity check completed. Review {'flagged' if contains_profanity else 'clean'} and saved to s3://{next_bucket}/{next_key}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Profanity check completed',
                'review_id': review_id,
                'reviewer_id': reviewer_id,
                'contains_profanity': contains_profanity,
                'reviewer_banned': is_banned,
                'profanity_review_count': current_profanity_review_count if contains_profanity else 0,
                'output_location': f"s3://{next_bucket}/{next_key}"
            })
        }

    except Exception as e:
        logger.error(f"Unhandled error during profanity check: {str(e)}. Stack trace: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Unhandled exception during profanity check',
                'details': str(e)
            })
        }
