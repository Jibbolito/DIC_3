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

# Initialize AWS clients
s3_client = boto3.client('s3')
# Initialize DynamoDB client, use endpoint_url for LocalStack
dynamodb = boto3.resource('dynamodb', endpoint_url=os.environ.get('AWS_ENDPOINT_URL'))
ssm_client = boto3.client('ssm', endpoint_url=os.environ.get('AWS_ENDPOINT_URL'))


# Initialize profanity filter
CUSTOM_PROFANITY_WORDS = {
    'scam', 'fake', 'fraud', 'ripoff', 'rip-off', 'con', 
    'cheat', 'steal', 'stealing', 'robbed', 'robbery',
    'garbage', 'trash', 'worthless', 'pathetic', 'useless'
}
pf = ProfanityFilter(extra_censor_list=list(CUSTOM_PROFANITY_WORDS))

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

# Get the DynamoDB table instance
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
            if orig != cens and '*' in cens:
                # Clean the original word of punctuation for detection
                clean_orig = re.sub(r'[^\w]', '', orig)
                if clean_orig:
                    profanity_words.append(clean_orig)
    
    # Calculate severity score based on profanity count and context
    severity_score = 0
    if contains_profanity:
        severity_score = len(profanity_words) * 2  # Base score
        
        # Add extra points for high-severity indicators
        text_lower = text.lower()
        high_severity_patterns = ['fuck', 'shit', 'bitch', 'damn']
        for pattern in high_severity_patterns:
            if pattern in text_lower:
                severity_score += 3
    
    return {
        'contains_profanity': contains_profanity,
        'profanity_words': list(set(profanity_words)),
        'profanity_count': len(set(profanity_words)),
        'severity_score': severity_score,
        'censored_text': censored_text
    }


def lambda_handler(event, context):
    """
    AWS Lambda handler for profanity checking
    
    Args:
        event: S3 event trigger from processed reviews
        context: Lambda context
        
    Returns:
        dict: Response with profanity analysis results
    """
    try:
        logger.info(f"Profanity check Lambda triggered with event: {json.dumps(event)}")
        
        # Get bucket and object information from S3 event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
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

        if contains_profanity:
            try:
                # Increment profanity count in DynamoDB
                response = customer_profanity_table.update_item(
                    Key={'reviewer_id': reviewer_id},
                    UpdateExpression='SET profanity_count = if_not_exists(profanity_count, :start) + :inc',
                    ExpressionAttributeValues={
                        ':start': 0,
                        ':inc': 1
                    },
                    ReturnValues='UPDATED_NEW'
                )
                current_profanity_count = int(response['Attributes'].get('profanity_count', 0))
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
        review_data['profanity_analysis']['current_reviewer_profanity_count'] = current_profanity_count if contains_profanity else 0

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