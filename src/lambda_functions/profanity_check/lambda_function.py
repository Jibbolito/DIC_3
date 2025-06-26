import json
import boto3
import logging
import re
from typing import Dict
import os
from profanityfilter import ProfanityFilter

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients with explicit region_name for robustness
# Ensure endpoint_url and credentials are set for LocalStack compatibility
REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1') # Define region
s3_client = boto3.client(
    's3',
    endpoint_url=os.environ.get('AWS_ENDPOINT_URL'), # Keep endpoint for LocalStack
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=REGION,
)
dynamodb = boto3.resource(
    'dynamodb',
    endpoint_url=os.environ.get('AWS_ENDPOINT_URL'), # Keep endpoint for LocalStack
    region_name=REGION,
)
ssm_client = boto3.client(
    'ssm',
    endpoint_url=os.environ.get('AWS_ENDPOINT_URL'), # Keep endpoint for LocalStack
    region_name=REGION,
)


# --- Load CUSTOM_PROFANITY_WORDS from external file ---
CUSTOM_PROFANITY_WORDS = set()
try:
    # Assuming custom_profanity.txt is in the same directory as lambda_function.py
    # which means it's at the root of the Lambda deployment package.
    with open('custom_profanity.txt', 'r', encoding='utf-8') as f:
        # Read each line, strip whitespace (including \r from Windows files)
        # and convert to a set for efficient lookup.
        CUSTOM_PROFANITY_WORDS = {line.strip() for line in f if line.strip()}
    logger.info(f"Loaded {len(CUSTOM_PROFANITY_WORDS)} custom profanity words from custom_profanity.txt")
except FileNotFoundError:
    logger.error("custom_profanity.txt not found. Using empty custom profanity words set.")
    # Fallback to an empty set if the file is not found
    CUSTOM_PROFANITY_WORDS = set()
except Exception as e:
    logger.error(f"Error loading custom_profanity.txt: {e}. Using empty custom profanity words set.")
    CUSTOM_PROFANITY_WORDS = set()


# Initialize profanity filter with custom words
pf = ProfanityFilter(extra_censor_list=list(CUSTOM_PROFANITY_WORDS)) # ProfanityFilter expects a list


def get_parameter(name):
    """Retrieves a parameter from AWS SSM Parameter Store."""
    try:
        response = ssm_client.get_parameter(Name=name, WithDecryption=True)
        return response['Parameter']['Value']
    except Exception as e:
        logger.error(f"Error retrieving SSM parameter {name}: {e}")
        raise

# Retrieve bucket and table names from SSM Parameter Store at cold start
try:
    FLAGGED_BUCKET = get_parameter('/my-app/s3/flagged_bucket_name')
    CLEAN_BUCKET = get_parameter('/my-app/s3/clean_bucket_name')
    CUSTOMER_PROFANITY_TABLE_NAME = get_parameter('/my-app/dynamodb/customer_profanity_table_name')
    BAN_THRESHOLD = int(get_parameter('/my-app/ban_threshold')) # Get ban threshold from SSM
except Exception as e:
    logger.error(f"Failed to load SSM parameters at initialization: {e}")
    # Fallback for local testing if SSM not setup
    FLAGGED_BUCKET = 'flagged-reviews-bucket'
    CLEAN_BUCKET = 'clean-reviews-bucket'
    CUSTOMER_PROFANITY_TABLE_NAME = 'CustomerProfanityCounts'
    BAN_THRESHOLD = 3 # Default ban threshold

# Get the DynamoDB table instance. Re-initialize it here to ensure it uses the
# potentially updated LOCALSTACK_ENDPOINT_URL or mocked DDB client.
# This pattern allows `dynamodb` variable to be properly set by mocks in tests.
customer_profanity_table = dynamodb.Table(CUSTOMER_PROFANITY_TABLE_NAME)

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
            'profanity_words': [],
            'profanity_count': 0,
            'severity_score': 0,
            'censored_text': ''
        }
    
    # Use profanityfilter to detect profanity
    contains_profanity = pf.is_profane(text)
    censored_text = pf.censor(text)
    
    # Extract profanity words by comparing original with censored
    profanity_words = []
    if contains_profanity:
        # Simple extraction by finding words that got censored
        original_words = text.lower().split()
        censored_words = censored_text.lower().split()
        
        for orig, cens in zip(original_words, censored_words):
            # Check if the word was censored (contains '*' and is not identical)
            if orig != cens and '*' in cens:
                # Clean the original word of punctuation for detection
                clean_orig = re.sub(r'[^\w]', '', orig)
                if clean_orig: # Ensure it's not an empty string after cleaning
                    profanity_words.append(clean_orig)
    
    # Calculate severity score based on profanity count and context
    severity_score = 0
    if contains_profanity:
        severity_score = len(profanity_words) * 2  # Base score
        
        # Add extra points for high-severity indicators (these are examples, customize as needed)
        text_lower = text.lower()
        high_severity_patterns = ['fuck', 'shit', 'bitch', 'damn'] # Example built-in high-severity
        for pattern in high_severity_patterns:
            if pattern in text_lower:
                severity_score += 3
        
        # Add points for custom profanity words (adjust multiplier as needed)
        for custom_word in CUSTOM_PROFANITY_WORDS:
            if custom_word in text_lower:
                severity_score += 1 # A smaller score for custom words, adjust based on desired impact

    
    return {
        'contains_profanity': contains_profanity,
        'profanity_words': list(set(profanity_words)), # Use set to ensure uniqueness
        'profanity_count': len(set(profanity_words)), # Count unique profanity words
        'severity_score': severity_score,
        'censored_text': censored_text
    }


def lambda_handler(event, context):
    """
    AWS Lambda handler for profanity checking
    
    Args:
        event: EventBridge S3 event trigger from processed reviews
        context: Lambda context
        
    Returns:
        dict: Response with profanity analysis results
    """
    try:
        logger.info(f"Profanity check Lambda triggered with event: {json.dumps(event)}")
        
        # S3 details are nested under the 'detail' key for EventBridge events.
        s3_detail = event.get('detail')
        if not s3_detail:
            raise ValueError("Event does not contain 'detail' key for S3 event.")
        
        bucket_name = s3_detail['bucket']['name']
        object_key = s3_detail['object']['key']
        
        logger.info(f"Processing file: {object_key} from bucket: {bucket_name}")
        
        # Download the processed review from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        review_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Perform profanity check on processed text fields
        summary_check = check_profanity_in_text(review_data.get('processed_summary', ''))
        reviewtext_check = check_profanity_in_text(review_data.get('processed_reviewText', ''))
        overall_check = check_profanity_in_text(review_data.get('processed_overall', ''))
        
        # Aggregate profanity results
        total_profanity_count = (
            summary_check['profanity_count'] + 
            reviewtext_check['profanity_count'] + 
            overall_check['profanity_count']
        )
        
        total_severity_score = (
            summary_check['severity_score'] + 
            reviewtext_check['severity_score'] + 
            overall_check['severity_score']
        )
        
        contains_profanity = (
            summary_check['contains_profanity'] or 
            reviewtext_check['contains_profanity'] or 
            overall_check['contains_profanity']
        )
        
        all_profanity_words = (
            summary_check['profanity_words'] + 
            reviewtext_check['profanity_words'] + 
            overall_check['profanity_words']
        )
        
        # Update review data with profanity analysis
        review_data.update({
            'processing_stage': 'profanity_checked',
            'profanity_analysis': {
                'contains_profanity': contains_profanity,
                'total_profanity_count': total_profanity_count,
                'total_severity_score': total_severity_score,
                'profanity_words': list(set(all_profanity_words)),
                'summary_profanity': summary_check,
                'reviewtext_profanity': reviewtext_check,
                'overall_profanity': overall_check
            }
        })
        
        # --- Profanity Count and Banning Logic ---
        reviewer_id = review_data.get('reviewer_id', 'unknown')
        is_banned = False
        current_profanity_count = 0 # Initialize to 0

        if contains_profanity:
            try:
                # Increment profanity count in DynamoDB
                # Atomically increments or sets to 1 if not exists
                update_response = customer_profanity_table.update_item(
                    Key={'reviewer_id': reviewer_id},
                    UpdateExpression='SET profanity_count = if_not_exists(profanity_count, :start) + :inc',
                    ExpressionAttributeValues={
                        ':start': 0,
                        ':inc': 1
                    },
                    ReturnValues='UPDATED_NEW' # Return the updated item
                )
                # Ensure we get the numeric value
                # DynamoDB returns numbers as Decimal or string in low-level API. Using .get('N') is safer.
                current_profanity_count = int(update_response['Attributes'].get('profanity_count', {'N': '0'})['N'])
                logger.info(f"Reviewer '{reviewer_id}' profanity count updated to: {current_profanity_count}")

                # Check for banning
                if current_profanity_count > BAN_THRESHOLD:
                    customer_profanity_table.update_item(
                        Key={'reviewer_id': reviewer_id},
                        UpdateExpression='SET is_banned = :val',
                        ExpressionAttributeValues={
                            ':val': True
                        }
                    )
                    is_banned = True
                    logger.warning(f"Reviewer '{reviewer_id}' has been banned due to excessive profanity ({current_profanity_count} reviews).")
            except Exception as ddb_e:
                logger.error(f"Error updating DynamoDB for reviewer '{reviewer_id}': {str(ddb_e)}")
                # Continue processing to S3 even if DynamoDB update fails
        
        review_data['profanity_analysis']['reviewer_banned'] = is_banned
        review_data['profanity_analysis']['current_reviewer_profanity_count'] = current_profanity_count

        # Determine next bucket based on profanity status
        if contains_profanity:
            next_bucket = FLAGGED_BUCKET
            next_key = f"flagged/{review_data.get('review_id', 'unknown')}.json"
        else:
            next_bucket = CLEAN_BUCKET
            next_key = f"clean/{review_data.get('review_id', 'unknown')}.json"
        
        # Save analyzed review to appropriate S3 bucket
        s3_client.put_object(
            Bucket=next_bucket,
            Key=next_key,
            Body=json.dumps(review_data, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Profanity check completed. Review {'flagged' if contains_profanity else 'clean'} and saved to {next_bucket}/{next_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Profanity check completed',
                'review_id': review_data.get('review_id', 'unknown'),
                'reviewer_id': review_data.get('reviewer_id', 'unknown'),
                'contains_profanity': contains_profanity,
                'profanity_count': total_profanity_count,
                'severity_score': total_severity_score,
                'reviewer_banned': is_banned,
                'output_location': f"s3://{next_bucket}/{next_key}"
            })
        }
        
    except Exception as e:
        logger.error(f"Error during profanity check: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to perform profanity check',
                'details': str(e)
            })
        }
