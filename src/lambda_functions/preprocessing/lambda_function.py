import json
import boto3
import nltk
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import logging
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')

# Download NLTK data (this happens at cold start)
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')
    nltk.download('wordnet')
    nltk.download('averaged_perceptron_tagger')

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
    text = re.sub(r'[^a-zA-Z0-9\s.,!?]', '', text)
    
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

def lambda_handler(event, context):
    """
    AWS Lambda handler for preprocessing reviews
    
    Args:
        event: S3 event trigger
        context: Lambda context
        
    Returns:
        dict: Response with status and processing details
    """
    try:
        logger.info(f"Preprocessing Lambda triggered with event: {json.dumps(event)}")
        
        # Get bucket and object information from S3 event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        logger.info(f"Processing file: {object_key} from bucket: {bucket_name}")
        
        # Download the review file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Handle both single JSON and JSONL formats
        try:
            # Try parsing as single JSON first
            review_data = json.loads(file_content)
        except json.JSONDecodeError:
            # If that fails, try parsing as JSONL (first line only for single review processing)
            lines = file_content.strip().split('\n')
            if lines:
                review_data = json.loads(lines[0])
            else:
                raise ValueError("Empty file or invalid format")
        
        # Process each field that needs analysis
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
        
        # Use environment variable for output bucket or default
        processed_bucket = os.environ.get('PROCESSED_BUCKET', 'processed-reviews-bucket')
        
        # Save processed review to S3 for next stage
        processed_key = f"processed/{object_key}"
        s3_client.put_object(
            Bucket=processed_bucket,
            Key=processed_key,
            Body=json.dumps(processed_review, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Successfully processed review and saved to {processed_bucket}/{processed_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Review successfully preprocessed',
                'review_id': processed_review['review_id'],
                'reviewer_id': processed_review['reviewer_id'],
                'total_words': processed_review['total_word_count'],
                'output_location': f"s3://{processed_bucket}/{processed_key}"
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing review: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to preprocess review',
                'details': str(e)
            })
        }