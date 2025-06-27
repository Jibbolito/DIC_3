#!/usr/bin/env python3
"""
Hybrid Serverless Solution - Uses LocalStack infrastructure with serverless processing logic
This processes the full dataset using the serverless architecture pattern
"""
import requests
import json
import time
from collections import defaultdict

class ServerlessProcessor:
    def __init__(self):
        self.base_url = "http://localhost:4566"
        self.buckets = [
            'raw-reviews-bucket',
            'processed-reviews-bucket', 
            'clean-reviews-bucket',
            'flagged-reviews-bucket',
            'final-reviews-bucket'
        ]
        
    def verify_infrastructure(self):
        """Verify LocalStack infrastructure is ready"""
        print("🔍 Verifying serverless infrastructure...")
        
        # Check LocalStack
        try:
            health = requests.get(f"{self.base_url}/_localstack/health")
            if health.status_code != 200:
                return False
            print("   ✅ LocalStack running")
        except:
            return False
        
        # Check S3 buckets
        bucket_count = 0
        for bucket in self.buckets:
            try:
                response = requests.get(f"{self.base_url}/{bucket}")
                if response.status_code == 200:
                    bucket_count += 1
            except:
                pass
        
        print(f"   📁 S3 Buckets: {bucket_count}/{len(self.buckets)} available")
        
        # Check DynamoDB
        try:
            response = requests.post(
                f"{self.base_url}/",
                headers={
                    "Content-Type": "application/x-amz-json-1.0",
                    "X-Amz-Target": "DynamoDB_20120810.DescribeTable"
                },
                json={"TableName": "CustomerProfanityCounts"}
            )
            dynamodb_ok = response.status_code == 200
            print(f"   🗃️  DynamoDB: {'✅ Ready' if dynamodb_ok else '❌ Not ready'}")
        except:
            dynamodb_ok = False
            print("   🗃️  DynamoDB: ❌ Error")
        
        return bucket_count >= 4 and dynamodb_ok
    
    def upload_dataset_to_s3(self):
        """Upload the dataset to raw bucket"""
        print("📤 Uploading dataset to raw-reviews-bucket...")
        
        try:
            with open('data/reviews_devset.json', 'r', encoding='utf-8') as f:
                content = f.read()
            
            response = requests.put(
                f"{self.base_url}/raw-reviews-bucket/full_dataset.json",
                data=content,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code in [200, 204]:
                print("   ✅ Dataset uploaded successfully")
                return True
            else:
                print(f"   ❌ Upload failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   ❌ Upload error: {e}")
            return False
    
    def simulate_preprocessing_lambda(self, review):
        """Simulate the preprocessing Lambda function"""
        try:
            reviewer_id = review.get('reviewerID', 'unknown')
            summary = review.get('summary', '')
            review_text = review.get('reviewText', '')
            overall = review.get('overall', 3)
            asin = review.get('asin', 'unknown')
            
            # Basic preprocessing (what Lambda would do)
            processed_review = {
                'reviewer_id': reviewer_id,
                'asin': asin,
                'original_summary': summary,
                'original_reviewText': review_text,
                'overall_rating': overall,
                'processing_stage': 'preprocessed',
                'timestamp': int(time.time())
            }
            
            return processed_review
        except:
            return None
    
    def simulate_profanity_lambda(self, processed_review):
        """Simulate the profanity check Lambda function"""
        try:
            # Profanity detection logic
            profane_words = [
                'damn', 'hell', 'crap', 'stupid', 'hate', 'terrible', 
                'awful', 'worst', 'horrible', 'garbage', 'trash', 'shit', 
                'fuck', 'bitch', 'suck', 'sucks', 'disappointing'
            ]
            
            text_to_check = (
                processed_review.get('original_summary', '') + ' ' + 
                processed_review.get('original_reviewText', '')
            ).lower()
            
            has_profanity = any(word in text_to_check for word in profane_words)
            
            processed_review['profanity_detected'] = has_profanity
            processed_review['processing_stage'] = 'profanity_checked'
            
            return processed_review, has_profanity
        except:
            return processed_review, False
    
    def simulate_sentiment_lambda(self, processed_review):
        """Simulate the sentiment analysis Lambda function"""
        try:
            overall_rating = processed_review.get('overall_rating', 3)
            
            if overall_rating >= 4:
                sentiment = 'positive'
            elif overall_rating <= 2:
                sentiment = 'negative'
            else:
                sentiment = 'neutral'
            
            processed_review['sentiment'] = sentiment
            processed_review['processing_stage'] = 'sentiment_analyzed'
            
            return processed_review, sentiment
        except:
            return processed_review, 'neutral'
    
    def store_in_s3(self, bucket, key, data):
        """Store data in S3 bucket via LocalStack"""
        try:
            response = requests.put(
                f"{self.base_url}/{bucket}/{key}",
                data=json.dumps(data),
                headers={"Content-Type": "application/json"}
            )
            return response.status_code in [200, 204]
        except:
            return False
    
    def update_dynamodb(self, reviewer_id, count):
        """Update DynamoDB via LocalStack"""
        try:
            item = {
                "TableName": "CustomerProfanityCounts",
                "Item": {
                    "reviewer_id": {"S": reviewer_id},
                    "unpolite_count": {"N": str(count)}
                }
            }
            
            response = requests.post(
                f"{self.base_url}/",
                headers={
                    "Content-Type": "application/x-amz-json-1.0",
                    "X-Amz-Target": "DynamoDB_20120810.PutItem"
                },
                json=item
            )
            
            return response.status_code == 200
        except:
            return False
    
    def process_full_dataset(self):
        """Process the complete dataset through serverless simulation"""
        print("🔄 Processing full dataset through serverless pipeline...")
        
        # Read dataset
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
        s3_operations = 0
        dynamodb_operations = 0
        
        print(f"   📊 Processing {len(lines)} reviews through serverless pipeline...")
        
        # Process each review
        for i, line in enumerate(lines):
            if not line.strip():
                continue
                
            try:
                review = json.loads(line.strip())
                total_reviews += 1
                
                # Step 1: Preprocessing Lambda simulation
                processed_review = self.simulate_preprocessing_lambda(review)
                if not processed_review:
                    continue
                
                # Store in processed bucket
                if self.store_in_s3('processed-reviews-bucket', f'processed_{i}.json', processed_review):
                    s3_operations += 1
                
                # Step 2: Profanity check Lambda simulation
                reviewed_data, has_profanity = self.simulate_profanity_lambda(processed_review)
                
                if has_profanity:
                    profanity_count += 1
                    reviewer_id = review.get('reviewerID', 'unknown')
                    user_profanity_counts[reviewer_id] += 1
                    
                    # Store in flagged bucket
                    if self.store_in_s3('flagged-reviews-bucket', f'flagged_{i}.json', reviewed_data):
                        s3_operations += 1
                    
                    # Update DynamoDB
                    if self.update_dynamodb(reviewer_id, user_profanity_counts[reviewer_id]):
                        dynamodb_operations += 1
                    
                    # Check for ban
                    if (user_profanity_counts[reviewer_id] > 3 and 
                        reviewer_id not in [u['user_id'] for u in banned_users]):
                        banned_users.append({
                            'user_id': reviewer_id,
                            'unpolite_count': user_profanity_counts[reviewer_id]
                        })
                else:
                    # Store in clean bucket
                    if self.store_in_s3('clean-reviews-bucket', f'clean_{i}.json', reviewed_data):
                        s3_operations += 1
                
                # Step 3: Sentiment analysis Lambda simulation
                final_review, sentiment = self.simulate_sentiment_lambda(reviewed_data)
                sentiment_counts[sentiment] += 1
                
                # Store final result
                if self.store_in_s3('final-reviews-bucket', f'final_{i}.json', final_review):
                    s3_operations += 1
                
                # Progress indicator
                if (i + 1) % 5000 == 0:
                    print(f"   📈 Processed {i+1:,} reviews... (S3: {s3_operations}, DB: {dynamodb_operations})")
                    
            except Exception as e:
                if i < 10:  # Only show first few errors
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
            "serverless_infrastructure_usage": {
                "s3_operations": s3_operations,
                "dynamodb_operations": dynamodb_operations,
                "buckets_used": self.buckets,
                "infrastructure": "LocalStack",
                "processing_pattern": "Event-driven serverless simulation"
            }
        }
        
        return results
    
    def verify_s3_results(self):
        """Verify results stored in S3 buckets"""
        print("🔍 Verifying data stored in S3 buckets...")
        
        verification = {}
        for bucket in self.buckets:
            try:
                response = requests.get(f"{self.base_url}/{bucket}")
                if response.status_code == 200:
                    import re
                    keys = re.findall(r'<Key>([^<]+)</Key>', response.text)
                    verification[bucket] = len(keys)
                    print(f"   📁 {bucket}: {len(keys)} files")
                else:
                    verification[bucket] = 0
                    print(f"   📭 {bucket}: Empty")
            except:
                verification[bucket] = -1
                print(f"   ❌ {bucket}: Error")
        
        return verification

def main():
    """Main function"""
    print("🚀 HYBRID SERVERLESS PROCESSING")
    print("📋 Processing complete dataset using LocalStack infrastructure")
    print("=" * 70)
    
    processor = ServerlessProcessor()
    
    # Verify infrastructure
    if not processor.verify_infrastructure():
        print("❌ Infrastructure not ready")
        return False
    
    # Upload dataset
    if not processor.upload_dataset_to_s3():
        print("❌ Failed to upload dataset")
        return False
    
    # Process dataset
    results = processor.process_full_dataset()
    
    if results:
        print("\n🎯 SERVERLESS PROCESSING COMPLETE!")
        print("=" * 70)
        print(f"📊 Total Reviews: {results['total_reviews']:,}")
        print(f"😊 Positive Reviews: {results['positive_reviews']:,} ({results['positive_reviews']/results['total_reviews']*100:.1f}%)")
        print(f"😐 Neutral Reviews: {results['neutral_reviews']:,} ({results['neutral_reviews']/results['total_reviews']*100:.1f}%)")
        print(f"😞 Negative Reviews: {results['negative_reviews']:,} ({results['negative_reviews']/results['total_reviews']*100:.1f}%)")
        print(f"🚫 Failed Profanity Check: {results['failed_profanity_check']:,} ({results['failed_profanity_check']/results['total_reviews']*100:.1f}%)")
        print(f"⛔ Banned Users: {results['banned_users_count']}")
        
        if results['banned_users']:
            print(f"\n🚫 Banned Users:")
            for user in results['banned_users']:
                print(f"   - {user['user_id']} ({user['unpolite_count']} unpolite reviews)")
        
        infra = results['serverless_infrastructure_usage']
        print(f"\n🏗️  LocalStack Infrastructure Usage:")
        print(f"   📁 S3 Operations: {infra['s3_operations']:,}")
        print(f"   🗃️  DynamoDB Operations: {infra['dynamodb_operations']:,}")
        print(f"   🌐 Infrastructure: {infra['infrastructure']}")
        print(f"   🔄 Processing Pattern: {infra['processing_pattern']}")
        
        # Verify S3 storage
        verification = processor.verify_s3_results()
        
        # Save results
        with open('hybrid_serverless_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n💾 Results saved to 'hybrid_serverless_results.json'")
        print("🎉 COMPLETE DATASET PROCESSED THROUGH LOCALSTACK INFRASTRUCTURE!")
        
        return True
    else:
        print("❌ Processing failed")
        return False

if __name__ == "__main__":
    main()