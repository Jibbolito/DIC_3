import json
import boto3
import re
import logging
import os
import uuid

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')

# Basic English stop words
STOP_WORDS = {
    'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from', 'has', 'he',
    'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the', 'to', 'was', 'will', 'with',
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours',
    'yourself', 'yourselves', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself',
    'they', 'them', 'their', 'theirs', 'themselves', 'this', 'these', 'those'
}

def preprocess_text(text):
    """Simple text preprocessing"""
    if not text or not isinstance(text, str):
        return {
            'original_text': text,
            'tokens': [],
            'processed_text': '',
            'word_count': 0
        }
    
    # Convert to lowercase and remove special characters
    text = text.lower()
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    
    # Basic tokenization - split by whitespace
    tokens = text.split()
    
    # Remove stop words and short words
    filtered_tokens = [word for word in tokens if word not in STOP_WORDS and len(word) > 2]
    
    # Basic stemming - remove common suffixes
    processed_tokens = []
    for word in filtered_tokens:
        if word.endswith('ing'):
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
    """AWS Lambda handler for preprocessing reviews"""
    processed_count = 0
    try:
        logger.info(f"Preprocessing Lambda triggered with event: {json.dumps(event)}")
        
        # Handle both EventBridge and direct invocation formats
        if 'detail' in event:
            # EventBridge format
            bucket_name = event['detail']['bucket']['name']
            object_key = event['detail']['object']['key']
        elif 'Records' in event:
            # Direct S3 event format
            bucket_name = event['Records'][0]['s3']['bucket']['name']
            object_key = event['Records'][0]['s3']['object']['key']
        else:
            # Manual invocation - use default test file
            bucket_name = 'raw-reviews-bucket'
            object_key = 'my_new_review.json'
            
        logger.info(f"Processing file: {object_key} from bucket: {bucket_name}")
        
        # Download the review file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Use environment variable for output bucket or default
        processed_bucket = os.environ.get('PROCESSED_BUCKET', 'processed-reviews-bucket')
        
        # Process each line as a separate review (JSONL format)
        for line_num, line in enumerate(file_content.strip().split('\n')):
            if line.strip():
                try:
                    review_data = json.loads(line.strip())
                    
                    # Process the review
                    processed_review = process_single_review(review_data, object_key)
                    
                    # Create unique key for each processed review
                    review_id_for_key = processed_review.get('review_id', str(uuid.uuid4()))
                    processed_key = f"processed/{review_id_for_key}_{line_num}.json"
                    
                    # Save processed review to S3
                    s3_client.put_object(
                        Bucket=processed_bucket,
                        Key=processed_key,
                        Body=json.dumps(processed_review, indent=2),
                        ContentType='application/json'
                    )
                    
                    processed_count += 1
                    
                    # Log progress for large files
                    if processed_count % 1000 == 0:
                        logger.info(f"Processed {processed_count} reviews so far...")
                        
                except json.JSONDecodeError:
                    logger.warning(f"Skipping invalid JSON on line {line_num}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing line {line_num}: {e}")
                    continue
        
        logger.info(f"Successfully processed {processed_count} reviews")
        
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

def process_single_review(review_data, original_object_key):
    """Helper function to process a single review"""
    processed_review = {
        'review_id': review_data.get('asin', 'unknown'),
        'reviewer_id': review_data.get('reviewerID', 'unknown'),
        'reviewer_name': review_data.get('reviewerName', ''),
        'overall_rating': review_data.get('overall', 0),
        'timestamp': review_data.get('unixReviewTime', 0),
        'category': review_data.get('category', ''),
        'helpful': review_data.get('helpful', [0, 0]),
        'original_summary': review_data.get('summary', ''),
        'original_reviewText': review_data.get('reviewText', ''),
        'original_overall': str(review_data.get('overall', '')),
        'original_object_key': original_object_key
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
    
    # Preprocess overall field
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