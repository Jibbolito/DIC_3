#!/usr/bin/env python3
"""
Set up the complete serverless infrastructure on LocalStack
"""
import requests
import json
import time

def create_s3_buckets():
    """Create all required S3 buckets"""
    print("📁 Creating S3 buckets...")
    
    buckets = [
        'raw-reviews-bucket',
        'processed-reviews-bucket', 
        'clean-reviews-bucket',
        'flagged-reviews-bucket',
        'final-reviews-bucket'
    ]
    
    created = 0
    for bucket in buckets:
        try:
            response = requests.put(f"http://localhost:4566/{bucket}")
            if response.status_code in [200, 409]:  # 409 = already exists
                print(f"   ✅ {bucket}")
                created += 1
            else:
                print(f"   ❌ {bucket}: {response.status_code}")
        except Exception as e:
            print(f"   ❌ {bucket}: {e}")
    
    return created

def create_dynamodb_table():
    """Create DynamoDB table for user profanity tracking"""
    print("🗃️  Creating DynamoDB table...")
    
    table_config = {
        "TableName": "CustomerProfanityCounts",
        "KeySchema": [
            {
                "AttributeName": "reviewer_id",
                "KeyType": "HASH"
            }
        ],
        "AttributeDefinitions": [
            {
                "AttributeName": "reviewer_id",
                "AttributeType": "S"
            }
        ],
        "BillingMode": "PAY_PER_REQUEST"
    }
    
    try:
        response = requests.post(
            "http://localhost:4566/",
            headers={
                "Content-Type": "application/x-amz-json-1.0",
                "X-Amz-Target": "DynamoDB_20120810.CreateTable"
            },
            json=table_config
        )
        
        if response.status_code in [200, 400]:  # 400 = already exists
            print("   ✅ CustomerProfanityCounts table created")
            return True
        else:
            print(f"   ❌ Table creation failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ DynamoDB error: {e}")
        return False

def test_lambda_functions():
    """Test all Lambda functions"""
    print("⚡ Testing Lambda functions...")
    
    functions = [
        "review-preprocessing-dev",
        "review-profanity-check-dev", 
        "review-sentiment-analysis-dev"
    ]
    
    test_event = {
        "Records": [{
            "s3": {
                "bucket": {"name": "test-bucket"},
                "object": {"key": "test-file.json"}
            }
        }]
    }
    
    working_functions = 0
    for func_name in functions:
        try:
            response = requests.post(
                f"http://localhost:4566/2015-03-31/functions/{func_name}/invocations",
                headers={"Content-Type": "application/json"},
                json=test_event
            )
            
            if response.status_code == 200:
                print(f"   ✅ {func_name}")
                working_functions += 1
            else:
                print(f"   ❌ {func_name}: {response.status_code}")
        except Exception as e:
            print(f"   ❌ {func_name}: {e}")
    
    return working_functions

def main():
    """Set up complete infrastructure"""
    print("🏗️  SETTING UP SERVERLESS INFRASTRUCTURE")
    print("=" * 60)
    
    # Create S3 buckets
    buckets_created = create_s3_buckets()
    
    # Create DynamoDB table
    dynamodb_ready = create_dynamodb_table()
    
    # Test Lambda functions
    print(f"\n⚡ TESTING LAMBDA FUNCTIONS")
    print("=" * 30)
    working_lambdas = test_lambda_functions()
    
    # Summary
    print(f"\n📊 INFRASTRUCTURE SUMMARY")
    print("=" * 60)
    print(f"   S3 Buckets: {buckets_created}/5 created")
    print(f"   DynamoDB: {'✅ Ready' if dynamodb_ready else '❌ Failed'}")
    print(f"   Lambda Functions: {working_lambdas}/3 working")
    
    if buckets_created == 5 and dynamodb_ready and working_lambdas == 3:
        print("\n🎉 INFRASTRUCTURE SETUP COMPLETE!")
        print("✅ All components ready for serverless processing")
        return True
    else:
        print("\n⚠️  Infrastructure setup incomplete")
        return False

if __name__ == "__main__":
    main()