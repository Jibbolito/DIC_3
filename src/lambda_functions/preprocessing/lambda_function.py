import json
import boto3
import nltk
import re
import os

nltk.data.path.append(os.path.join(os.path.dirname(__file__), 'nltk_data'))

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import logging
import uuid


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')



def preprocess_text(text):
    """
    Preprocess text by performing tokenization, stop word removal, and lemmatization
    
    Args:
        text (str): Raw text to preprocess
        
    Returns:
        dict: Dictionary containing original text, tokens, and processed text
    """
    if not text or not isinstance(text, str):
        return {
            'original_text': text,
            'tokens': [],
            'processed_text': '',
            'word_count': 0
        }
    
    # Convert to lowercase and remove special characters (except basic punctuation)
    text = text.lower()
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    
    # Tokenization
    tokens = word_tokenize(text)
    
    # Remove stop words
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word not in stop_words and len(word) > 2]
    
    # Lemmatization
    lemmatizer = WordNetLemmatizer()
    lemmatized_tokens = [lemmatizer.lemmatize(word) for word in filtered_tokens]
    
    # Join processed tokens back into text
    processed_text = ' '.join(lemmatized_tokens)
    
    return {
        'original_text': text,
        'tokens': lemmatized_tokens,
        'processed_text': processed_text,
        'word_count': len(lemmatized_tokens)
    }

def process_single_review(review_data: dict, original_object_key: str) -> dict:
    """
    Helper function to perform preprocessing on a single review dictionary.
    """
    processed_review = {
        'review_id': review_data.get('asin', 'unknown'), # 'asin' is often used as product ID, can be review ID
        'reviewer_id': review_data.get('reviewerID', 'unknown'),
        'reviewer_name': review_data.get('reviewerName', ''),
        'overall_rating': review_data.get('overall', 0),
        'timestamp': review_data.get('unixReviewTime', 0),
        'category': review_data.get('category', ''),
        'helpful': review_data.get('helpful', [0, 0]),
        'original_summary': review_data.get('summary', ''),
        'original_reviewText': review_data.get('reviewText', ''),
        'original_overall': str(review_data.get('overall', '')),
        'original_object_key': original_object_key # Keep track of original source file
    }
    
    # Preprocess summary field
    if processed_review['original_summary']:
        summary_result = preprocess_text(processed_review['original_summary'])
        processed_review['processed_summary'] = summary_result['processed_text']
        processed_review['summary_tokens'] = summary_result['tokens']
        processed_review['summary_word_count'] = summary_result['word_count']
    else:
        processed_review['processed_summary'] = ''
        processed_review['summary_tokens'] = []
        processed_review['summary_word_count'] = 0
    
    # Preprocess reviewText field
    if processed_review['original_reviewText']:
        reviewtext_result = preprocess_text(processed_review['original_reviewText'])
        processed_review['processed_reviewText'] = reviewtext_result['processed_text']
        processed_review['reviewText_tokens'] = reviewtext_result['tokens']
        processed_review['reviewText_word_count'] = reviewtext_result['word_count']
    else:
        processed_review['processed_reviewText'] = ''
        processed_review['reviewText_tokens'] = []
        processed_review['reviewText_word_count'] = 0
    
    # Preprocess overall field (convert to text and process)
    if processed_review['original_overall']:
        overall_result = preprocess_text(processed_review['original_overall'])
        processed_review['processed_overall'] = overall_result['processed_text']
        processed_review['overall_tokens'] = overall_result['tokens']
        processed_review['overall_word_count'] = overall_result['word_count']
    else:
        processed_review['processed_overall'] = ''
        processed_review['overall_tokens'] = []
        processed_review['overall_word_count'] = 0
    
    # Add processing metadata
    processed_review['processing_stage'] = 'preprocessed'
    processed_review['total_word_count'] = (
        processed_review['summary_word_count'] + 
        processed_review['reviewText_word_count'] + 
        processed_review['overall_word_count']
    )
    return processed_review

def lambda_handler(event, context):
    """
    AWS Lambda handler for preprocessing reviews
    
    Args:
        event: S3 event trigger
        context: Lambda context
        
    Returns:
        dict: Response with status and processing details
    """
    # Initialize counters
    processed_count = 0
    errors_count = 0

    try:
        logger.info(f"Preprocessing Lambda triggered with event: {json.dumps(event)}")
        
        # Get bucket and object information from S3 event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        logger.info(f"Processing file: {object_key} from bucket: {bucket_name}")
        
        # Download the review file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')

        processed_bucket = os.environ.get('PROCESSED_BUCKET', 'processed-reviews-bucket')

        
        # Handle both single JSON and JSONL formats
        reviews_to_process = []
        try:
            # Try parsing as single JSON first
            reviews_to_process.append(json.loads(file_content))
        except json.JSONDecodeError:
            # If that fails, assume JSONL and process each line
            lines = file_content.strip().split('\n')
            for line in lines:
                if line.strip(): # Ensure line is not empty
                    try:
                        reviews_to_process.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.error(f"Skipping malformed JSON line in {object_key}: {line[:100]}... Error: {e}")
                        errors_count += 1 # Increment errors_count for malformed lines
                        continue
        
        # Determine a base path for processed files to avoid collisions if multiple JSONL files
        # are uploaded (e.g., reviews-batch1.jsonl, reviews-batch2.jsonl)
        original_file_prefix = os.path.splitext(object_key)[0] # e.g., 'my_new_review' from 'my_new_review.json'

        # Process each field that needs analysis
        for review_data in reviews_to_process:
            processed_review = process_single_review(review_data, object_key)

            # Create a unique key for each processed review,
            # using 'asin' or a generated UUID, and including the original file's context.
            review_id_for_key = processed_review.get('review_id') or str(uuid.uuid4())
            # Changed key to include original file prefix for better organization and collision avoidance
            processed_key = f"processed/{original_file_prefix}/{review_id_for_key}.json" 

            s3_client.put_object(
                Bucket=processed_bucket,
                Key=processed_key,
                Body=json.dumps(processed_review, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Successfully processed review '{review_id_for_key}' and saved to {processed_bucket}/{processed_key}")
            processed_count += 1
        
        return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'{processed_count} review(s) successfully preprocessed and sent to {processed_bucket}',
                    'processed_count': processed_count,
                    'errors_count': errors_count # Include errors_count in the response
                })
            }
        
    except Exception as e:
        logger.error(f"Error processing reviews: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to preprocess reviews',
                'details': str(e),
                'processed_count': processed_count, # Return counts even on general error
                'errors_count': errors_count
            })
        }
