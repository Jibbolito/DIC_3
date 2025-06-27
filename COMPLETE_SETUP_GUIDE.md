# Complete Serverless Architecture Setup Guide

## 🚀 Full LocalStack Setup with Lambda Functions - Complete Data Processing

This guide will help you run the complete serverless review processing system on LocalStack with all 78,829 reviews using real Lambda functions.

---

## 📋 Prerequisites

- **Docker Desktop** installed and running
- **Python 3.x** installed
- **PowerShell** (Windows) or Terminal (Mac/Linux)
- **Git** (to clone the project)

---

## 🐳 Step 1: Start LocalStack Container

Open a **new PowerShell window** and run:

```powershell
# Start LocalStack with all required services
docker run --rm -it -p 4566:4566 -p 4510-4559:4510-4559 -e SERVICES=lambda,s3,dynamodb,iam,logs,events localstack/localstack
```

**Keep this window open** - LocalStack needs to stay running.

**Expected output:**
```
LocalStack version: 4.x.x
Starting services: lambda,s3,dynamodb,iam,logs,events
Ready.
```

---

## 📁 Step 2: Setup Project Directory

Open a **second PowerShell window** and navigate to your project:

```powershell
# Navigate to your project directory
cd C:\path\to\your\DIC2025_Assignment3

# OR clone if needed:
# git clone <your-repo-url>
# cd DIC2025_Assignment3
```

---

## ✅ Step 3: Verify LocalStack is Ready

```powershell
# Check if LocalStack is running
docker ps | findstr localstack

# Check LocalStack health
curl http://localhost:4566/_localstack/health
```

**Expected**: JSON response showing all services as "running"

---

## 🏗️ Step 4: Setup Infrastructure

```powershell
# Create S3 buckets and DynamoDB table
python setup_infrastructure.py
```

**Expected output:**
```
🏗️  SETTING UP SERVERLESS INFRASTRUCTURE
============================================================
📁 Creating S3 buckets...
   ✅ raw-reviews-bucket
   ✅ processed-reviews-bucket
   ✅ clean-reviews-bucket
   ✅ flagged-reviews-bucket
   ✅ final-reviews-bucket
🗃️  Creating DynamoDB table...
   ✅ CustomerProfanityCounts table created
```

---

## ⚡ Step 5: Deploy Lambda Functions

```powershell
# Deploy the 3 Lambda functions to LocalStack
python deploy_simple_lambdas.py
```

**Expected output:**
```
🚀 DEPLOYING SIMPLE LAMBDA FUNCTIONS
============================================================
🚀 Deploying review-preprocessing-dev...
   ✅ review-preprocessing-dev deployed successfully
🚀 Deploying review-profanity-check-dev...
   ✅ review-profanity-check-dev deployed successfully
🚀 Deploying review-sentiment-analysis-dev...
   ✅ review-sentiment-analysis-dev deployed successfully
```

---

## 🎯 Step 6: Quick Test (Optional - 2 minutes)

Test with 10,000 reviews first:

```powershell
python final_serverless_test.py
```

**Expected**: Process 10,000 reviews successfully with results showing sentiment analysis and profanity detection.

---

## 🚀 Step 7: Process Complete Dataset (5-10 minutes)

Process all 78,829 reviews through the serverless architecture:

```powershell
python final_working_solution.py
```

**Expected output:**
```
🚀 FINAL COMPLETE DATASET PROCESSING
📋 Processing 78,829 reviews through LocalStack infrastructure
======================================================================
✅ LocalStack verified and ready
🔧 Setting up infrastructure...
   📁 S3 Buckets: 5/5
   🗃️  DynamoDB: ✅
📤 Uploading complete dataset to LocalStack S3...
   ✅ Dataset upload successful
🔄 Processing COMPLETE 78,829 review dataset through LocalStack...
   📊 Processing 78829 reviews through serverless infrastructure...
   📈 Processed 5,000 reviews... (S3: 15000, DB: 512)
   📈 Processed 10,000 reviews... (S3: 30000, DB: 1214)
   ...continues until all 78,829 reviews processed...
```

**Processing time**: 5-10 minutes depending on your machine

---

## 📊 Step 8: Verify Results

After completion, check the results:

```powershell
# View complete assignment results
type assignment_results_final.json

# View detailed processing results
type complete_localstack_results.json

# Check files stored in LocalStack S3
curl http://localhost:4566/final-reviews-bucket
```

---

## 🎯 Expected Final Results

**Complete Dataset Results:**
- **Total Reviews**: 78,829
- **Positive Reviews**: 62,196 (78.9%)
- **Neutral Reviews**: 6,644 (8.4%)
- **Negative Reviews**: 9,989 (12.7%)
- **Failed Profanity Check**: 5,389 (6.8%)
- **Banned Users**: 3 users with 4+ unpolite reviews

**Infrastructure Usage:**
- **S3 Operations**: ~300,000+ API calls
- **DynamoDB Operations**: ~5,000+ API calls
- **Files Stored**: 78,829+ across 5 S3 buckets

---

## 🔧 Architecture Components Verified

✅ **3 Lambda Functions**:
- `review-preprocessing-dev` - Text preprocessing with tokenization
- `review-profanity-check-dev` - Content filtering and user tracking  
- `review-sentiment-analysis-dev` - Sentiment classification

✅ **5 S3 Buckets**:
- `raw-reviews-bucket` - Input data
- `processed-reviews-bucket` - Preprocessed data
- `clean-reviews-bucket` - Non-profane content
- `flagged-reviews-bucket` - Profane content
- `final-reviews-bucket` - Complete results

✅ **1 DynamoDB Table**:
- `CustomerProfanityCounts` - User behavior tracking

✅ **Event-Driven Processing**:
- S3 events trigger Lambda functions
- Automatic data flow through buckets
- Scalable serverless architecture

---

## 🛠️ Troubleshooting

**LocalStack not starting?**
```powershell
# Check if port 4566 is available
netstat -an | findstr 4566

# Stop any existing LocalStack
docker stop $(docker ps -q --filter ancestor=localstack/localstack)

# Restart LocalStack
docker run --rm -it -p 4566:4566 -p 4510-4559:4510-4559 localstack/localstack
```

**Python dependencies missing?**
```powershell
pip install requests
```

**Data file not found?**
Ensure `data/reviews_devset.json` exists in your project directory.

**Lambda functions not deploying?**
Wait 30 seconds after starting LocalStack, then retry deployment.

---

## 📝 Alternative Processing Options

**For Real AWS Lambda Functions:**
```powershell
python real_serverless_processor.py
```

**For Hybrid Serverless Simulation:**
```powershell
python hybrid_serverless_solution.py
```

**For Full Dataset with Different Processing:**
```powershell
python full_dataset_serverless.py
```

---

## 🎉 Success Verification

You'll know everything worked when you see:

1. ✅ All services running in LocalStack
2. ✅ All infrastructure components created
3. ✅ All 78,829 reviews processed
4. ✅ Results files generated with correct statistics
5. ✅ Files stored in LocalStack S3 buckets
6. ✅ User behavior tracked in DynamoDB

**Total processing time**: ~10-15 minutes from start to finish

**Files generated**:
- `assignment_results_final.json` - Assignment results
- `complete_localstack_results.json` - Detailed processing results
- `final_serverless_test.json` - Test run results

Your serverless architecture is now fully tested and verified! 🚀