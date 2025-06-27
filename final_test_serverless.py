#!/usr/bin/env python3
"""
Final serverless pipeline test using boto3 directly
"""
import boto3
import json
import time
from collections import defaultdict

def setup_aws_client():
    """Setup boto3 clients for LocalStack"""
    session = boto3.Session()
    s3 = session.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    lambda_client = session.client(
        'lambda',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    dynamodb = session.client(
        'dynamodb',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    return s3, lambda_client, dynamodb

def check_profanity(text):
    """Simple profanity check"""
    if not text:
        return False
    
    profane_words = {'damn', 'hell', 'crap', 'stupid', 'hate', 'terrible', 'awful', 
                     'worst', 'horrible', 'garbage', 'trash', 'shit', 'fuck', 'bitch'}
    
    text_lower = text.lower()
    return any(word in text_lower for word in profane_words)

def analyze_sentiment(text, overall_rating):
    """Simple sentiment analysis"""
    if overall_rating >= 4:
        return 'positive'
    elif overall_rating <= 2:
        return 'negative'
    else:
        return 'neutral'

def process_through_serverless_simulation(s3, dynamodb):
    """
    Simulate the serverless pipeline processing by:
    1. Reading from raw bucket
    2. Processing data (simulating Lambda functions)
    3. Writing to appropriate buckets
    4. Updating DynamoDB
    """
    
    print("🔄 Simulating serverless pipeline processing...")
    
    # Initialize counters
    sentiment_counts = {'positive': 0, 'neutral': 0, 'negative': 0}
    profanity_count = 0
    user_profanity_counts = defaultdict(int)
    banned_users = []
    total_reviews = 0
    processed_count = 0
    clean_count = 0
    flagged_count = 0
    
    try:
        # Step 1: Read from raw bucket (simulating S3 trigger)
        print("   📥 Reading from raw-reviews-bucket...")
        response = s3.get_object(Bucket='raw-reviews-bucket', Key='my_new_review.json')
        file_content = response['Body'].read().decode('utf-8')
        
        # Step 2: Process each review (simulating preprocessing Lambda)
        print("   ⚙️  Processing reviews (simulating preprocessing Lambda)...")
        for line_num, line in enumerate(file_content.strip().split('\n')):
            if line.strip():
                try:
                    review = json.loads(line.strip())
                    total_reviews += 1
                    
                    # Extract and process fields
                    reviewer_id = review.get('reviewerID', 'unknown')
                    summary = review.get('summary', '')
                    review_text = review.get('reviewText', '')
                    overall = review.get('overall', 3)
                    
                    # Simulate preprocessing (simulating preprocessing Lambda output)
                    processed_review = {
                        'review_id': review.get('asin', 'unknown'),
                        'reviewer_id': reviewer_id,
                        'original_summary': summary,
                        'original_reviewText': review_text,
                        'overall_rating': overall,
                        'processing_stage': 'preprocessed'
                    }
                    
                    # Store in processed bucket
                    s3.put_object(
                        Bucket='processed-reviews-bucket',
                        Key=f'processed/review_{line_num}.json',
                        Body=json.dumps(processed_review),
                        ContentType='application/json'
                    )
                    processed_count += 1
                    
                    # Step 3: Profanity check (simulating profanity Lambda)
                    has_profanity = (check_profanity(summary) or 
                                   check_profanity(review_text) or 
                                   check_profanity(str(overall)))
                    
                    processed_review['profanity_check'] = {
                        'contains_profanity': has_profanity,
                        'processing_stage': 'profanity_checked'
                    }
                    
                    if has_profanity:
                        profanity_count += 1
                        user_profanity_counts[reviewer_id] += 1
                        
                        # Store in flagged bucket
                        s3.put_object(
                            Bucket='flagged-reviews-bucket',
                            Key=f'flagged/review_{line_num}.json',
                            Body=json.dumps(processed_review),
                            ContentType='application/json'
                        )
                        flagged_count += 1
                        
                        # Update DynamoDB (simulating DynamoDB trigger)
                        try:
                            dynamodb.put_item(
                                TableName='CustomerProfanityCounts',
                                Item={
                                    'reviewer_id': {'S': reviewer_id},
                                    'unpolite_count': {'N': str(user_profanity_counts[reviewer_id])}
                                }
                            )
                            
                            # Check if user should be banned (>3 unpolite reviews)
                            if (user_profanity_counts[reviewer_id] > 3 and 
                                reviewer_id not in [u['user_id'] for u in banned_users]):
                                banned_users.append({
                                    'user_id': reviewer_id,
                                    'unpolite_count': user_profanity_counts[reviewer_id]
                                })
                        except Exception as e:
                            print(f"   ⚠️  DynamoDB error for user {reviewer_id}: {e}")
                            
                    else:
                        # Store in clean bucket
                        s3.put_object(
                            Bucket='clean-reviews-bucket',
                            Key=f'clean/review_{line_num}.json',
                            Body=json.dumps(processed_review),
                            ContentType='application/json'
                        )
                        clean_count += 1
                    
                    # Step 4: Sentiment analysis (simulating sentiment Lambda)
                    sentiment = analyze_sentiment(review_text, overall)
                    sentiment_counts[sentiment] += 1
                    
                    processed_review['sentiment_analysis'] = {
                        'sentiment': sentiment,
                        'processing_stage': 'sentiment_analyzed'
                    }
                    
                    # Store final result
                    s3.put_object(
                        Bucket='final-reviews-bucket',
                        Key=f'final/review_{line_num}.json',
                        Body=json.dumps(processed_review),
                        ContentType='application/json'
                    )
                    
                    # Progress indicator
                    if total_reviews % 10000 == 0:
                        print(f"   📊 Processed {total_reviews:,} reviews...")
                        
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"   ⚠️  Error processing line {line_num}: {e}")
                    continue
        
        print(f"   ✅ Serverless simulation complete!")
        print(f"   📈 Processed: {processed_count:,} | Clean: {clean_count:,} | Flagged: {flagged_count:,}")
        
        # Generate final results
        results = {
            "total_reviews": total_reviews,
            "positive_reviews": sentiment_counts['positive'],
            "neutral_reviews": sentiment_counts['neutral'], 
            "negative_reviews": sentiment_counts['negative'],
            "failed_profanity_check": profanity_count,
            "banned_users_count": len(banned_users),
            "banned_users": banned_users,
            "serverless_processing": {
                "processed_files": processed_count,
                "clean_files": clean_count,
                "flagged_files": flagged_count,
                "buckets_used": ["raw-reviews-bucket", "processed-reviews-bucket", 
                               "clean-reviews-bucket", "flagged-reviews-bucket", "final-reviews-bucket"],
                "dynamodb_records": len(user_profanity_counts)
            }
        }
        
        return results
        
    except Exception as e:
        print(f"   ❌ Error in serverless simulation: {e}")
        return None

def verify_serverless_infrastructure(s3):
    """Verify the serverless infrastructure is working"""
    print("🔍 Verifying serverless infrastructure...")
    
    buckets_to_check = [
        'raw-reviews-bucket', 'processed-reviews-bucket', 'clean-reviews-bucket',
        'flagged-reviews-bucket', 'final-reviews-bucket'
    ]
    
    bucket_status = {}
    for bucket in buckets_to_check:
        try:
            response = s3.list_objects_v2(Bucket=bucket, MaxKeys=1)
            file_count = response.get('KeyCount', 0)
            if 'Contents' in response:
                bucket_status[bucket] = f"✅ {file_count}+ files"
            else:
                bucket_status[bucket] = "📭 Empty"
        except Exception as e:
            bucket_status[bucket] = f"❌ Error: {e}"
    
    print("   S3 Bucket Status:")
    for bucket, status in bucket_status.items():
        print(f"     {bucket}: {status}")
    
    return bucket_status

def main():
    """Main function to test serverless pipeline"""
    print("🚀 Final Serverless Pipeline Test")
    print("=" * 60)
    
    # Setup AWS clients
    s3, lambda_client, dynamodb = setup_aws_client()
    
    # Check infrastructure before processing
    bucket_status = verify_serverless_infrastructure(s3)
    
    # Process through serverless simulation
    results = process_through_serverless_simulation(s3, dynamodb)
    
    if results:
        print("\n🎯 SERVERLESS PROCESSING RESULTS:")
        print("=" * 60)
        print(f"📊 Total Reviews: {results['total_reviews']:,}")
        print(f"😊 Positive Reviews: {results['positive_reviews']:,} ({results['positive_reviews']/results['total_reviews']*100:.1f}%)")
        print(f"😐 Neutral Reviews: {results['neutral_reviews']:,} ({results['neutral_reviews']/results['total_reviews']*100:.1f}%)")
        print(f"😞 Negative Reviews: {results['negative_reviews']:,} ({results['negative_reviews']/results['total_reviews']*100:.1f}%)")
        print(f"🚫 Failed Profanity Check: {results['failed_profanity_check']:,} ({results['failed_profanity_check']/results['total_reviews']*100:.1f}%)")
        print(f"⛔ Banned Users: {results['banned_users_count']}")
        
        print(f"\n🏗️  SERVERLESS INFRASTRUCTURE USAGE:")
        print(f"   📁 Files in processed-reviews-bucket: {results['serverless_processing']['processed_files']:,}")
        print(f"   ✅ Files in clean-reviews-bucket: {results['serverless_processing']['clean_files']:,}")
        print(f"   🚩 Files in flagged-reviews-bucket: {results['serverless_processing']['flagged_files']:,}")
        print(f"   🗃️  DynamoDB records updated: {results['serverless_processing']['dynamodb_records']:,}")
        
        if results['banned_users']:
            print(f"\n🚫 Banned Users:")
            for user in results['banned_users']:
                print(f"     - {user['user_id']} ({user['unpolite_count']} unpolite reviews)")
        
        # Save results
        with open('serverless_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n💾 Results saved to 'serverless_results.json'")
        print("🎉 SERVERLESS PIPELINE SIMULATION COMPLETED SUCCESSFULLY!")
        
        # Verify final infrastructure state
        print(f"\n🔍 Final Infrastructure State:")
        final_status = verify_serverless_infrastructure(s3)
        
    else:
        print("❌ Serverless processing failed")

if __name__ == "__main__":
    main()