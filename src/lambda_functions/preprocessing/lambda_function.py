import json
import boto3
import re
import logging
import os
import uuid # For generating unique IDs

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client(
    's3',
    endpoint_url=os.environ.get('AWS_ENDPOINT_URL', 'http://localhost:4566'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', 'test'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'test'),
    region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
)

# --- Load STOP_WORDS from external file ---
STOP_WORDS = set()
try:
    # Assuming stopwords.txt is in the same directory as lambda_function.py
    # which means it's at the root of the Lambda deployment package.
    with open('stopwords.txt', 'r', encoding='utf-8') as f:
        # Read each line, strip whitespace (including \r from Windows files)
        # and convert to a set for efficient lookup.
        STOP_WORDS = {line.strip() for line in f if line.strip()}
    logger.info(f"Loaded {len(STOP_WORDS)} stopwords from stopwords.txt")
except FileNotFoundError:
    logger.error("stopwords.txt not found. Using empty stop words set.")
    # Fallback to an empty set if the file is not found
    STOP_WORDS = set()
except Exception as e:
    logger.error(f"Error loading stopwords.txt: {e}. Using empty stop words set.")
    STOP_WORDS = set()


def preprocess_text(text):
    """
    Preprocess text by performing:
    1. Lowercasing
    2. Removal of all punctuation
    3. Tokenization
    4. Stop word removal using the loaded STOP_WORDS set
    5. Basic stemming
    
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
    
    # Convert to lowercase
    text = text.lower()
    
    # --- FIX: Remove all punctuation ---
    # This regex replaces anything that is NOT a letter, number, or whitespace with an empty string.
    text = re.sub(r'[^\w\s]', '', text)
    
    # Tokenization - split by whitespace (after punctuation removal)
    tokens = text.split()
    
    # Remove stop words and short words (length 2 or less)
    # Using the externally loaded STOP_WORDS set
    filtered_tokens = [word for word in tokens if word not in STOP_WORDS and len(word) > 2]
    
    # Basic stemming - remove common suffixes
    processed_tokens = []
    for word in filtered_tokens:
        if word.endswith('ing'):
            # Example: 'running' -> 'runn' (not 'run')
            word = word[:-3] + 'n' if word.endswith('ing') and len(word) > 3 else word[:-3] # A more nuanced example, or just word[:-3]
            # Simple fix: just remove 'ing'
            word = word[:-3]
        elif word.endswith('ed'):
            word = word[:-2]
        elif word.endswith('er'):
            word = word[:-2]
        elif word.endswith('ly'):
            word = word[:-2]
        processed_tokens.append(word)
    
    # Join processed tokens back into text
    processed_text = ' '.join(processed_tokens)
    
    return {
        'original_text': text,
        'tokens': processed_tokens,
        'processed_text': processed_text,
        'word_count': len(processed_tokens)
    }

def lambda_handler(event, context):
    """
    AWS Lambda handler for preprocessing reviews
    
    Args:
        event: EventBridge S3 event trigger
        context: Lambda context
        
    Returns:
        dict: Response with status and processing details
    """
    processed_count = 0
    file_content = "" # Initialize for logging purposes if error occurs early
    try:
        logger.info(f"Preprocessing Lambda triggered with event: {json.dumps(event)}")
        
        # S3 details are nested under the 'detail' key for EventBridge events.
        s3_detail = event.get('detail')
        if not s3_detail:
            raise ValueError("Event does not contain 'detail' key for S3 event.")
        
        bucket_name = s3_detail['bucket']['name']
        object_key = s3_detail['object']['key']
            
        logger.info(f"Processing file: {object_key} from bucket: {bucket_name}")
        
        # Download the review file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')
        
        reviews_to_process = []
        parsing_error = False
        try:
            # Try parsing as single JSON first
            reviews_to_process.append(json.loads(file_content))
        except json.JSONDecodeError:
            # If that fails, try parsing as JSONL (each line is a separate JSON object)
            for line in file_content.strip().split('\n'):
                if line:
                    try:
                        reviews_to_process.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON line in {object_key}: {line} - {e}")
                        parsing_error = True # Mark that a parsing error occurred
        
        # --- FIX: Return 500 for invalid JSON input if no reviews could be parsed ---
        if not reviews_to_process and len(file_content) > 0:
            error_message = f"Input file '{object_key}' contains no valid JSON reviews or is malformed."
            if parsing_error:
                error_message += " One or more lines failed JSON parsing."
            logger.error(error_message)
            return {
                'statusCode': 500, # Return 500 for genuinely unparsable input
                'body': json.dumps({'error': 'Failed to preprocess reviews due to invalid input format', 'details': error_message})
            }
        elif not reviews_to_process and len(file_content) == 0:
             logger.warning(f"Input file '{object_key}' is empty. No reviews to process.")
             return {
                'statusCode': 200, # Still 200 for genuinely empty file as it's not an error to handle
                'body': json.dumps({'message': f'Input file is empty. No reviews to process in {object_key}'})
             }
        # --- END FIX ---


        processed_bucket = os.environ.get('PROCESSED_BUCKET', 'processed-reviews-bucket')

        for review_data in reviews_to_process:
            processed_review = process_single_review(review_data, object_key)

            review_id_for_key = processed_review.get('review_id', str(uuid.uuid4()))
            processed_key = f"processed/{review_id_for_key}.json"

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
                'processed_count': processed_count
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing reviews: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to preprocess reviews',
                'details': str(e)
            })
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
