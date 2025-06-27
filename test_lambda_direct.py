#!/usr/bin/env python3
"""
Direct test of Lambda function using HTTP calls to LocalStack
"""
import requests
import json
import base64

def create_simple_function():
    """Create a simple Lambda function directly via LocalStack API"""
    
    # Read the deployment package
    try:
        with open('deployments/preprocessing_simple.zip', 'rb') as f:
            function_code = base64.b64encode(f.read()).decode('utf-8')
    except FileNotFoundError:
        print("❌ Error: preprocessing_simple.zip not found")
        return False
    
    # Function configuration
    function_config = {
        "FunctionName": "review-preprocessing-simple",
        "Runtime": "python3.10",
        "Role": "arn:aws:iam::000000000000:role/lambda-role",
        "Handler": "lambda_function.lambda_handler",
        "Code": {
            "ZipFile": function_code
        },
        "Description": "Simple preprocessing function",
        "Timeout": 300,
        "MemorySize": 512,
        "Environment": {
            "Variables": {
                "PROCESSED_BUCKET": "processed-reviews-bucket"
            }
        }
    }
    
    try:
        # Create function via LocalStack API
        response = requests.post(
            "http://localhost:4566/2015-03-31/functions",
            headers={"Content-Type": "application/json"},
            json=function_config
        )
        
        if response.status_code == 201:
            print("✅ Successfully created simple preprocessing function")
            return True
        else:
            print(f"❌ Failed to create function: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error creating function: {e}")
        return False

def invoke_function():
    """Invoke the Lambda function directly"""
    
    # Test event
    test_event = {
        "detail-type": "Object Created",
        "source": "aws.s3",
        "detail": {
            "bucket": {"name": "raw-reviews-bucket"},
            "object": {"key": "my_new_review.json"}
        }
    }
    
    try:
        # Invoke function via LocalStack API
        response = requests.post(
            "http://localhost:4566/2015-03-31/functions/review-preprocessing-simple/invocations",
            headers={"Content-Type": "application/json"},
            json=test_event
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Function invoked successfully!")
            print(f"Response: {json.dumps(result, indent=2)}")
            return True
        else:
            print(f"❌ Function invocation failed: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error invoking function: {e}")
        return False

def check_processed_files():
    """Check if files were created in processed bucket"""
    try:
        response = requests.get("http://localhost:4566/processed-reviews-bucket")
        if response.status_code == 200:
            print("✅ Processed bucket contents:")
            # Parse XML response to get object list
            if "<Key>" in response.text:
                import re
                keys = re.findall(r'<Key>([^<]+)</Key>', response.text)
                for key in keys[:5]:  # Show first 5 files
                    print(f"   - {key}")
                print(f"   ... and {max(0, len(keys)-5)} more files")
                return len(keys)
            else:
                print("   No files found")
                return 0
        else:
            print(f"❌ Failed to check bucket: {response.status_code}")
            return 0
    except Exception as e:
        print(f"❌ Error checking bucket: {e}")
        return 0

def main():
    """Main test function"""
    print("🚀 Testing simple Lambda function deployment...")
    print("=" * 50)
    
    # Step 1: Create function
    print("Step 1: Creating simple Lambda function...")
    if not create_simple_function():
        return
    
    print("\nStep 2: Invoking function...")
    if not invoke_function():
        return
    
    print("\nStep 3: Checking processed files...")
    file_count = check_processed_files()
    
    if file_count > 0:
        print(f"\n🎉 SUCCESS! Processed {file_count} files through serverless pipeline!")
    else:
        print("\n⚠️  Function ran but no processed files found. Check logs for details.")

if __name__ == "__main__":
    main()