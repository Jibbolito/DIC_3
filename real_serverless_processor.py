#!/usr/bin/env python3
"""
REAL Serverless Processing - Uses actual Lambda functions on LocalStack
This processes the full dataset through deployed Lambda functions
"""
import requests
import json
import time
import base64
import zipfile
import io
from collections import defaultdict

def create_working_lambda_code():
    """Create Lambda function code that actually processes data"""
    
    preprocessing_code = '''
import json
import re
from collections import defaultdict

def lambda_handler(event, context):
    """Real preprocessing Lambda that processes review data"""
    try:
        # Get review data from event
        if 'review_data' in event:
            review = event['review_data']
        else:
            # This would normally read from S3, but for testing we'll use event data
            review = {"reviewText": "sample", "summary": "sample", "overall": 3}
        
        # Real preprocessing
        reviewer_id = review.get('reviewerID', 'unknown')
        summary = review.get('summary', '')
        review_text = review.get('reviewText', '')
        overall = review.get('overall', 3)
        
        # Basic text processing
        text_to_process = (summary + ' ' + review_text).lower()
        processed_text = re.sub(r'[^a-zA-Z0-9\\s]', '', text_to_process)
        
        # Create processed result
        processed_review = {
            'reviewer_id': reviewer_id,
            'original_summary': summary,
            'original_text': review_text,
            'processed_text': processed_text,
            'overall_rating': overall,
            'processing_stage': 'preprocessed',
            'timestamp': str(context.aws_request_id) if context else 'test'
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Preprocessing completed',
                'processed_review': processed_review
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
'''

    profanity_code = '''
import json

def lambda_handler(event, context):
    """Real profanity check Lambda"""
    try:
        # Get processed review from event
        if 'processed_review' in event:
            review = event['processed_review']
        else:
            return {'statusCode': 400, 'body': json.dumps({'error': 'No review data'})}
        
        # Profanity word list
        profane_words = [
            'damn', 'hell', 'crap', 'stupid', 'hate', 'terrible', 
            'awful', 'worst', 'horrible', 'garbage', 'trash', 'shit', 
            'fuck', 'bitch', 'suck', 'sucks', 'bad', 'disappointing'
        ]
        
        # Check for profanity
        text_to_check = (review.get('original_summary', '') + ' ' + 
                        review.get('original_text', '')).lower()
        
        has_profanity = any(word in text_to_check for word in profane_words)
        
        # Update review with profanity check
        review['profanity_detected'] = has_profanity
        review['processing_stage'] = 'profanity_checked'
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Profanity check completed',
                'review': review,
                'profanity_detected': has_profanity
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
'''

    sentiment_code = '''
import json

def lambda_handler(event, context):
    """Real sentiment analysis Lambda"""
    try:
        # Get review from event
        if 'review' in event:
            review = event['review']
        else:
            return {'statusCode': 400, 'body': json.dumps({'error': 'No review data'})}
        
        # Simple sentiment analysis based on rating
        overall_rating = review.get('overall_rating', 3)
        
        if overall_rating >= 4:
            sentiment = 'positive'
        elif overall_rating <= 2:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        # Update review with sentiment
        review['sentiment'] = sentiment
        review['processing_stage'] = 'sentiment_analyzed'
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Sentiment analysis completed',
                'review': review,
                'sentiment': sentiment
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
'''
    
    return preprocessing_code, profanity_code, sentiment_code

def create_lambda_zip(code):
    """Create a ZIP package for Lambda function"""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr('lambda_function.py', code)
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def deploy_real_lambda(function_name, code):
    """Deploy a real working Lambda function"""
    print(f"🚀 Deploying {function_name}...")
    
    zip_data = create_lambda_zip(code)
    encoded_zip = base64.b64encode(zip_data).decode('utf-8')
    
    function_config = {
        "FunctionName": function_name,
        "Runtime": "python3.10",
        "Role": "arn:aws:iam::000000000000:role/lambda-role",
        "Handler": "lambda_function.lambda_handler",
        "Code": {"ZipFile": encoded_zip},
        "Description": f"Real working {function_name}",
        "Timeout": 300,  # 5 minutes
        "MemorySize": 512
    }
    
    try:
        # Delete existing function
        requests.delete(f"http://localhost:4566/2015-03-31/functions/{function_name}")
        
        # Deploy new function
        response = requests.post(
            "http://localhost:4566/2015-03-31/functions",
            headers={"Content-Type": "application/json"},
            json=function_config
        )
        
        if response.status_code in [200, 201]:
            print(f"   ✅ {function_name} deployed successfully")
            # Wait for function to be ready
            time.sleep(2)
            return True
        else:
            print(f"   ❌ Deployment failed: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"   ❌ Deployment error: {e}")
        return False

def invoke_lambda(function_name, payload):
    """Invoke a Lambda function and return the result"""
    try:
        response = requests.post(
            f"http://localhost:4566/2015-03-31/functions/{function_name}/invocations",
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"   ⚠️  Lambda {function_name} failed: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"   ⚠️  Lambda {function_name} error: {e}")
        return None

def store_in_s3(bucket, key, data):
    """Store data in S3 bucket"""
    try:
        response = requests.put(
            f"http://localhost:4566/{bucket}/{key}",
            data=json.dumps(data),
            headers={"Content-Type": "application/json"}
        )
        return response.status_code in [200, 204]
    except:
        return False

def update_dynamodb(reviewer_id, count):
    """Update DynamoDB with user profanity count"""
    try:
        item = {
            "TableName": "CustomerProfanityCounts",
            "Item": {
                "reviewer_id": {"S": reviewer_id},
                "unpolite_count": {"N": str(count)}
            }
        }
        
        response = requests.post(
            "http://localhost:4566/",
            headers={
                "Content-Type": "application/x-amz-json-1.0",
                "X-Amz-Target": "DynamoDB_20120810.PutItem"
            },
            json=item
        )
        
        return response.status_code == 200
    except:
        return False

def process_full_dataset_real_serverless():
    """Process the complete dataset using REAL Lambda functions"""
    print("🔄 Processing full dataset through REAL serverless Lambda functions...")
    
    # Read the dataset
    try:
        with open('data/reviews_devset.json', 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"❌ Failed to read dataset: {e}")
        return None
    
    # Initialize counters
    sentiment_counts = {'positive': 0, 'neutral': 0, 'negative': 0}
    profanity_count = 0
    user_profanity_counts = defaultdict(int)
    banned_users = []
    total_reviews = 0
    successful_lambda_calls = 0
    
    print(f"   📊 Processing {len(lines)} reviews through Lambda functions...")
    
    # Process each review through the Lambda pipeline
    for i, line in enumerate(lines):
        if not line.strip():
            continue
            
        try:
            review = json.loads(line.strip())
            total_reviews += 1
            
            # Step 1: Invoke preprocessing Lambda
            preprocessing_result = invoke_lambda('review-preprocessing-dev', {
                'review_data': review
            })
            
            if preprocessing_result and preprocessing_result.get('statusCode') == 200:
                body = json.loads(preprocessing_result['body'])
                processed_review = body['processed_review']
                successful_lambda_calls += 1
                
                # Store in processed bucket
                store_in_s3('processed-reviews-bucket', f'processed_{i}.json', processed_review)
                
                # Step 2: Invoke profanity check Lambda
                profanity_result = invoke_lambda('review-profanity-check-dev', {
                    'processed_review': processed_review
                })
                
                if profanity_result and profanity_result.get('statusCode') == 200:
                    profanity_body = json.loads(profanity_result['body'])
                    reviewed_data = profanity_body['review']
                    has_profanity = profanity_body['profanity_detected']
                    successful_lambda_calls += 1
                    
                    if has_profanity:
                        profanity_count += 1
                        reviewer_id = review.get('reviewerID', 'unknown')
                        user_profanity_counts[reviewer_id] += 1
                        
                        # Store in flagged bucket
                        store_in_s3('flagged-reviews-bucket', f'flagged_{i}.json', reviewed_data)
                        
                        # Update DynamoDB
                        update_dynamodb(reviewer_id, user_profanity_counts[reviewer_id])
                        
                        # Check for ban
                        if (user_profanity_counts[reviewer_id] > 3 and 
                            reviewer_id not in [u['user_id'] for u in banned_users]):
                            banned_users.append({
                                'user_id': reviewer_id,
                                'unpolite_count': user_profanity_counts[reviewer_id]
                            })
                    else:
                        # Store in clean bucket
                        store_in_s3('clean-reviews-bucket', f'clean_{i}.json', reviewed_data)
                    
                    # Step 3: Invoke sentiment analysis Lambda
                    sentiment_result = invoke_lambda('review-sentiment-analysis-dev', {
                        'review': reviewed_data
                    })
                    
                    if sentiment_result and sentiment_result.get('statusCode') == 200:
                        sentiment_body = json.loads(sentiment_result['body'])
                        final_review = sentiment_body['review']
                        sentiment = sentiment_body['sentiment']
                        successful_lambda_calls += 1
                        
                        sentiment_counts[sentiment] += 1
                        
                        # Store final result
                        store_in_s3('final-reviews-bucket', f'final_{i}.json', final_review)
            
            # Progress indicator
            if (i + 1) % 1000 == 0:
                print(f"   📈 Processed {i+1:,} reviews... (Lambda calls: {successful_lambda_calls})")
                
        except Exception as e:
            print(f"   ⚠️  Error processing review {i}: {e}")
            continue
    
    # Generate final results
    results = {
        "total_reviews": total_reviews,
        "positive_reviews": sentiment_counts['positive'],
        "neutral_reviews": sentiment_counts['neutral'],
        "negative_reviews": sentiment_counts['negative'],
        "failed_profanity_check": profanity_count,
        "banned_users_count": len(banned_users),
        "banned_users": banned_users,
        "lambda_execution_stats": {
            "total_lambda_calls": successful_lambda_calls,
            "reviews_processed": total_reviews,
            "success_rate": f"{(successful_lambda_calls/(total_reviews*3))*100:.1f}%" if total_reviews > 0 else "0%"
        },
        "infrastructure": "LocalStack with Real Lambda Functions"
    }
    
    return results

def main():
    """Main function"""
    print("🚀 REAL SERVERLESS LAMBDA PROCESSING")
    print("📋 Processing complete dataset through actual Lambda functions")
    print("=" * 70)
    
    # Deploy real working Lambda functions
    preprocessing_code, profanity_code, sentiment_code = create_working_lambda_code()
    
    functions = [
        ('review-preprocessing-dev', preprocessing_code),
        ('review-profanity-check-dev', profanity_code),
        ('review-sentiment-analysis-dev', sentiment_code)
    ]
    
    print("🔧 Deploying real Lambda functions...")
    deployed = 0
    for func_name, code in functions:
        if deploy_real_lambda(func_name, code):
            deployed += 1
    
    if deployed != 3:
        print(f"❌ Only {deployed}/3 functions deployed successfully")
        return False
    
    print(f"✅ All {deployed}/3 Lambda functions deployed successfully")
    
    # Process the full dataset
    results = process_full_dataset_real_serverless()
    
    if results:
        print("\n🎯 REAL SERVERLESS PROCESSING COMPLETE!")
        print("=" * 70)
        print(f"📊 Total Reviews: {results['total_reviews']:,}")
        print(f"😊 Positive Reviews: {results['positive_reviews']:,}")
        print(f"😐 Neutral Reviews: {results['neutral_reviews']:,}")
        print(f"😞 Negative Reviews: {results['negative_reviews']:,}")
        print(f"🚫 Failed Profanity Check: {results['failed_profanity_check']:,}")
        print(f"⛔ Banned Users: {results['banned_users_count']}")
        
        if results['banned_users']:
            print(f"\n🚫 Banned Users:")
            for user in results['banned_users']:
                print(f"   - {user['user_id']} ({user['unpolite_count']} unpolite reviews)")
        
        stats = results['lambda_execution_stats']
        print(f"\n⚡ Lambda Execution Stats:")
        print(f"   Total Lambda Calls: {stats['total_lambda_calls']:,}")
        print(f"   Reviews Processed: {stats['reviews_processed']:,}")
        print(f"   Success Rate: {stats['success_rate']}")
        
        # Save results
        with open('real_serverless_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n💾 Results saved to 'real_serverless_results.json'")
        print("🎉 COMPLETE DATASET PROCESSED THROUGH REAL LAMBDA FUNCTIONS!")
        
        return True
    else:
        print("❌ Real serverless processing failed")
        return False

if __name__ == "__main__":
    main()