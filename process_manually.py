import json
import boto3
import os

# Set up AWS client for LocalStack
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', 
                  aws_access_key_id='test', aws_secret_access_key='test',
                  region_name='us-east-1')

lambda_client = boto3.client('lambda', endpoint_url='http://localhost:4566',
                           aws_access_key_id='test', aws_secret_access_key='test', 
                           region_name='us-east-1')

print("Processing dataset through Lambda functions...")

# Trigger preprocessing function for the dataset
event = {
    'detail-type': 'Object Created',
    'source': 'aws.s3',
    'detail': {
        'bucket': {'name': 'raw-reviews-bucket'},
        'object': {'key': 'my_new_review.json'}
    }
}

try:
    response = lambda_client.invoke(
        FunctionName='review-preprocessing-dev',
        Payload=json.dumps(event)
    )
    print("Preprocessing function invoked successfully")
    result = json.loads(response['Payload'].read())
    print("Response:", result)
except Exception as e:
    print(f"Error: {e}")