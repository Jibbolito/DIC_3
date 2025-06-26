import json
import boto3
import logging
import os
import re

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
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
# Simple sentiment keywords for basic analysis
POSITIVE_WORDS = {
    'good', 'great', 'excellent', 'amazing', 'awesome', 'wonderful', 'fantastic', 
    'love', 'like', 'best', 'perfect', 'happy', 'pleased', 'satisfied', 'recommend',
    'beautiful', 'nice', 'helpful', 'useful', 'quality', 'fast', 'easy', 'quick'
}

NEGATIVE_WORDS = {
    'bad', 'terrible', 'awful', 'horrible', 'hate', 'dislike', 'worst', 'poor',
    'disappointed', 'unsatisfied', 'broken', 'defective', 'slow', 'difficult',
    'expensive', 'cheap', 'useless', 'waste', 'problem', 'issue', 'wrong', 'error'
}

def get_parameter(name):
    """Retrieves a parameter from AWS SSM Parameter Store."""
    try:
        response = ssm_client.get_parameter(Name=name, WithDecryption=True)
        return response['Parameter']['Value']
    except Exception as e:
        logger.error(f"Error retrieving SSM parameter {name}: {e}")
        raise

# Retrieve bucket names from SSM Parameter Store at cold start
try:
    FINAL_REVIEWS_BUCKET = get_parameter('/my-app/s3/final_reviews_bucket_name')
except Exception as e:
    logger.error(f"Failed to load SSM parameters at initialization: {e}")
    # Fallback or re-raise based on your error handling strategy
    FINAL_REVIEWS_BUCKET = 'final-reviews-bucket' # Fallback for local testing if SSM not setup

def analyze_sentiment_in_text(text: str) -> dict:
    """
    Analyze sentiment of text using simple keyword matching.
    
    Args:
        text (str): Text to analyze.
        
    Returns:
        dict: Sentiment analysis scores (neg, neu, pos, compound).
    """
    if not text or not isinstance(text, str):
        return {'neg': 0, 'neu': 0, 'pos': 0, 'compound': 0}
    
    # Convert to lowercase and split into words
    words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
    
    positive_count = sum(1 for word in words if word in POSITIVE_WORDS)
    negative_count = sum(1 for word in words if word in NEGATIVE_WORDS)
    total_words = len(words)
    
    if total_words == 0:
        return {'neg': 0, 'neu': 1, 'pos': 0, 'compound': 0}
    
    # Calculate simple scores
    pos_score = positive_count / total_words
    neg_score = negative_count / total_words
    neu_score = max(0, 1 - pos_score - neg_score)
    
    # Simple compound score calculation
    compound = (positive_count - negative_count) / max(total_words, 1)
    compound = max(-1, min(1, compound))  # Clamp between -1 and 1
    
    return {
        'neg': round(neg_score, 3),
        'neu': round(neu_score, 3), 
        'pos': round(pos_score, 3),
        'compound': round(compound, 3)
    }

def lambda_handler(event, context):
    """
    AWS Lambda handler for sentiment analysis.
    
    Args:
        event: EventBridge S3 event trigger from processed/profanity-checked reviews.
        context: Lambda context.
        
    Returns:
        dict: Response with sentiment analysis results.
    """
    try:
        logger.info(f"Sentiment analysis Lambda triggered with event: {json.dumps(event)}")
        
        # --- FIX: Parsing EventBridge S3 Event structure ---
        s3_detail = event.get('detail')
        if not s3_detail:
            raise ValueError("Event does not contain 'detail' key for S3 event.")
        
        bucket_name = s3_detail['bucket']['name']
        object_key = s3_detail['object']['key']
        # --- END FIX ---
        
        logger.info(f"Analyzing sentiment for file: {object_key} from bucket: {bucket_name}")
        
        # Download the review data from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        review_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # Perform sentiment analysis on processed text fields
        summary_sentiment = analyze_sentiment_in_text(review_data.get('processed_summary', ''))
        reviewtext_sentiment = analyze_sentiment_in_text(review_data.get('processed_reviewText', ''))
        overall_sentiment = analyze_sentiment_in_text(review_data.get('processed_overall', ''))
        
        # Aggregate sentiment results (e.g., using a weighted average of compound scores)
        total_word_count = review_data.get('total_word_count', 0)
        
        weighted_compound_sum = (
            (summary_sentiment['compound'] * review_data.get('summary_word_count', 0)) +
            (reviewtext_sentiment['compound'] * review_data.get('reviewText_word_count', 0)) +
            (overall_sentiment['compound'] * review_data.get('overall_word_count', 0))
        )
        
        overall_compound_sentiment = weighted_compound_sum / (total_word_count if total_word_count > 0 else 1)
        
        sentiment_label = 'neutral'
        if overall_compound_sentiment >= 0.05:
            sentiment_label = 'positive'
        elif overall_compound_sentiment <= -0.05:
            sentiment_label = 'negative'
            
        # Update review data with sentiment analysis
        review_data.update({
            'processing_stage': 'sentiment_analyzed',
            'sentiment_analysis': {
                'summary_sentiment': summary_sentiment,
                'reviewtext_sentiment': reviewtext_sentiment,
                'overall_sentiment': overall_sentiment,
                'aggregated_compound_score': overall_compound_sentiment,
                'sentiment_label': sentiment_label
            }
        })
        
        # Save analyzed review to the final S3 bucket
        final_key = f"analyzed/{review_data.get('review_id', 'unknown')}.json"
        s3_client.put_object(
            Bucket=FINAL_REVIEWS_BUCKET,
            Key=final_key,
            Body=json.dumps(review_data, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Sentiment analysis completed. Review saved to {FINAL_REVIEWS_BUCKET}/{final_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Sentiment analysis completed',
                'review_id': review_data.get('review_id', 'unknown'),
                'reviewer_id': review_data.get('reviewer_id', 'unknown'),
                'sentiment_label': sentiment_label,
                'output_location': f"s3://{FINAL_REVIEWS_BUCKET}/{final_key}"
            })
        }
        
    except Exception as e:
        logger.error(f"Error during sentiment analysis: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to perform sentiment analysis',
                'details': str(e)
            })
        }
