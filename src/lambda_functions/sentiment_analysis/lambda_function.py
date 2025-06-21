import json
import boto3
import logging
import os
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', endpoint_url=os.environ.get('AWS_ENDPOINT_URL')) # Use endpoint_url for LocalStack
ssm_client = boto3.client('ssm', endpoint_url=os.environ.get('AWS_ENDPOINT_URL')) # Use endpoint_url for LocalStack


# Download NLTK data (for VADER sentiment analysis)
# This part is crucial for Lambda cold starts. Ensure these are downloaded to /tmp if packaging.
try:
    nltk.data.find('sentiment/vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon', download_dir='/tmp')
    nltk.data.path.append('/tmp') # Add /tmp to NLTK data path

# Initialize VADER sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

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
    Analyze sentiment of text using VADER.
    
    Args:
        text (str): Text to analyze.
        
    Returns:
        dict: Sentiment analysis scores (neg, neu, pos, compound).
    """
    if not text or not isinstance(text, str):
        return {'neg': 0, 'neu': 0, 'pos': 0, 'compound': 0}
        
    return analyzer.polarity_scores(text)

def lambda_handler(event, context):
    """
    AWS Lambda handler for sentiment analysis.
    
    Args:
        event: S3 event trigger from processed/profanity-checked reviews.
        context: Lambda context.
        
    Returns:
        dict: Response with sentiment analysis results.
    """
    try:
        logger.info(f"Sentiment analysis Lambda triggered with event: {json.dumps(event)}")
        
        # Get bucket and object information from S3 event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
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