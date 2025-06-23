# DIC2025 Assignment 3 - My Lambda Functions

## Overview

This repository contains my contribution to the serverless review analysis application - specifically the preprocessing and profanity check Lambda functions.

## My Responsibilities

1. **Preprocessing Lambda Function** - Tokenization, stop word removal, lemmatization
2. **Profanity Check Lambda Function** - Flag reviews containing bad words
3. **Unit/Integration Tests** - For both functions
4. **S3 Integration** - Ensure both functions handle S3 input/output correctly

## Project Structure

```
DIC2025_Assignment3/
├── src/
│   ├── lambda_functions/
│   │   ├── preprocessing/
│   │   │   ├── lambda_function.py
│   │   │   └── requirements.txt
│   │   └── profanity_check/
│   │   |   ├── lambda_function.py
│   │   |   └── requirements.txt
│   │   └── sentiment_analysis/
│   │       ├── lambda_function.py
│   │       └── requirements.txt
│   └── tests/
│       ├── test_preprocessing.py
│       ├── test_profanity_check.py
│       └── test_integration.py
├── data/
│   └── reviews_devset.json        # Real dataset (78,829 reviews)
├── deployments/                   # Lambda deployment packages
│   ├── preprocessing_deployment.zip
│   ├── profanity_check_deployment.zip
│   └── sentiment_analysis_deployment.zip
├── analyze_devset_simple.py       # Analysis script for results
├── package_lambdas.py             # Lambda packaging script
├── setup_environment.sh           # Environment setup automation
├── setup_aws_lambdas.sh           # Deploy Lambda functions to LocalStack
├── setup_eventbridge.sh           # Configure EventBridge triggers
├── run_on_reviews_devset.sh       # Test pipeline with real dataset
└── README.md
```

## Lambda Functions

### Preprocessing Function
- **Input**: Raw review JSON from S3
- **Processing**: Tokenization, stop word removal, lemmatization using NLTK
- **Output**: Processed review JSON to S3

### Profanity Check Function  
- **Input**: Processed review JSON from S3
- **Processing**: Uses `profanityfilter` package (as recommended in tips & tricks) + custom words
- **Output**: Flagged/clean review JSON to different S3 buckets

## Testing

- Unit tests for individual function logic
- Integration tests for S3 input/output handling
- Mock AWS services for testing

## Requirements

### Prerequisites
- **Python 3.10+** (3.9+ compatible)
- **Docker Desktop** (for LocalStack)
- **Git** (for bash scripts on Windows)

### Platform-Specific Setup

## Windows Setup

### Prerequisites Installation
1. **Install Python 3.10+**: Download from [python.org](https://www.python.org/downloads/)
2. **Install Docker Desktop**: Download from [docker.com](https://www.docker.com/products/docker-desktop/)
3. **Install Git for Windows**: Download from [git-scm.com](https://git-scm.com/downloads) (includes Git Bash)

### Environment Setup (Windows)
```powershell
# 1. Navigate to project directory
cd C:\path\to\DIC2025_Assignment3

# 2. Create and activate virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1

# 3. Install dependencies (Windows-compatible method)
python install_requirements.py
# OR if you prefer manual installation:
# pip install -r requirements.txt

# 4. Start Docker Desktop (ensure it's running)
docker --version

# 5. Test the analysis (works without AWS CLI)
python analyze_devset_simple.py

# 6. For full AWS LocalStack testing (requires Git Bash):
# Option A: Use Git Bash for shell scripts
& "C:\Program Files\Git\bin\bash.exe" ./setup_environment.sh
python package_lambdas.py
& "C:\Program Files\Git\bin\bash.exe" ./setup_aws_lambdas.sh

# Option B: Manual LocalStack testing
docker run --rm -d --name localstack -p 4566:4566 -e SERVICES=s3,lambda,events,iam localstack/localstack
# Then test S3 uploads:
Invoke-WebRequest -Uri "http://localhost:4566/test-bucket" -Method PUT
```

## macOS Setup

### Prerequisites Installation
```bash
# Install Homebrew (if not installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Python 3.10+
brew install python@3.10

# Install Docker Desktop
brew install --cask docker

# Install additional utilities
brew install jq curl
```

### Environment Setup (macOS)
```bash
# 1. Navigate to project directory
cd /path/to/DIC2025_Assignment3

# 2. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
pip install requests

# 4. Start Docker Desktop and verify
docker --version

# 5. Test the analysis (works without AWS CLI)
python analyze_devset_simple.py

# 6. Full AWS LocalStack automated setup
chmod +x setup_environment.sh
./setup_environment.sh
python package_lambdas.py
chmod +x setup_aws_lambdas.sh
./setup_aws_lambdas.sh
chmod +x setup_eventbridge.sh
./setup_eventbridge.sh
```

## Linux Setup

### Prerequisites Installation
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3.10 python3.10-venv python3-pip docker.io jq curl

# CentOS/RHEL
sudo yum install python3 python3-pip docker jq curl

# Start Docker
sudo systemctl start docker
sudo usermod -aG docker $USER  # Add user to docker group
```

### Environment Setup (Linux)
```bash
# 1. Navigate to project directory
cd /path/to/DIC2025_Assignment3

# 2. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
pip install requests

# 4. Verify Docker
docker --version

# 5. Test the analysis
python analyze_devset_simple.py

# 6. Full AWS LocalStack automated setup
chmod +x setup_environment.sh setup_aws_lambdas.sh setup_eventbridge.sh
./setup_environment.sh
python package_lambdas.py
./setup_aws_lambdas.sh
./setup_eventbridge.sh
```

## Quick Start Guide

### Minimum Working Setup (All Platforms)
```bash
# 1. Clone and navigate to project
cd DIC2025_Assignment3

# 2. Create virtual environment
python -m venv .venv

# 3. Activate virtual environment
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# Linux/macOS:
source .venv/bin/activate

# 4. Install dependencies
pip install -r requirements.txt
pip install requests

# 5. Test core functionality (no AWS required)
python analyze_devset_simple.py
```

### Full AWS LocalStack Testing
**Prerequisites**: Docker Desktop running

**Windows (with Git Bash)**:
```powershell
docker run --rm -d --name localstack -p 4566:4566 -e SERVICES=s3,lambda,events,iam localstack/localstack
& "C:\Program Files\Git\bin\bash.exe" ./setup_aws_lambdas.sh
```

**macOS/Linux**:
```bash
docker run --rm -d --name localstack -p 4566:4566 -e SERVICES=s3,lambda,events,iam localstack/localstack
chmod +x setup_aws_lambdas.sh && ./setup_aws_lambdas.sh
```

## Environment Verification

After setup, verify everything is working:

```bash
# Quick verification
python verify_setup.py

# Test with real dataset
python analyze_devset_simple.py
```

## Testing & Analysis

### Run Unit/Integration Tests
```bash
# Run all tests
python run_tests.py

# Test specific components
python run_tests.py preprocessing    # Preprocessing function only
python run_tests.py profanity_check  # Profanity check function only  
python run_tests.py integration      # S3 integration only
```

### Run AWS LocalStack Pipeline Test
```bash
# Test complete pipeline with real dataset
chmod +x run_on_reviews_devset.sh
./run_on_reviews_devset.sh
```

### Run Dataset Analysis (Alternative)
```bash
# Analyze the full dataset (78,829 reviews) without AWS
python analyze_devset_simple.py
```

**Analysis Results Generated:**
- Sentiment analysis: 78.9% positive, 8.4% neutral, 12.7% negative reviews
- Profanity detection: 8.1% of reviews failed profanity check
- User bans: 4 users banned for >3 unpolite reviews
- Results automatically saved to `assignment_results.json`

## Environment Variables

### Preprocessing Function
- `PROCESSED_BUCKET` - S3 bucket for processed reviews (default: processed-reviews-bucket)

### Profanity Check Function  
- `CLEAN_BUCKET` - S3 bucket for clean reviews (default: clean-reviews-bucket)
- `FLAGGED_BUCKET` - S3 bucket for flagged reviews (default: flagged-reviews-bucket)

## EventBridge Setup (Tips & Tricks Implementation)

For triggering multiple functions with the same S3 event (as recommended in tips & tricks):

```bash
# Make script executable and run
chmod +x setup_eventbridge.sh
./setup_eventbridge.sh
```

This sets up EventBridge to trigger multiple Lambda functions from a single S3 event, following the recommended pattern from the tips & tricks document.

## AWS LocalStack Deployment

The project now supports full AWS LocalStack deployment for testing the complete serverless pipeline:

### Quick Start
```bash
# Ensure Docker is running, then:
./setup_environment.sh
python package_lambdas.py  
./setup_aws_lambdas.sh
./run_on_reviews_devset.sh
```

### Deployed Components
- **Lambda Functions**: preprocessing, profanity_check, sentiment_analysis
- **S3 Buckets**: raw-reviews, processed-reviews, clean-reviews, flagged-reviews
- **IAM Roles**: lambda-role with proper permissions
- **LocalStack Services**: S3, Lambda, IAM, Events

### Monitoring
```bash
# Check LocalStack health
curl http://localhost:4566/_localstack/health

# View LocalStack logs  
localstack logs -f
```

## Troubleshooting

### Common Issues

**Windows PowerShell Script Execution**:
```powershell
# If script execution is disabled:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**Docker Permission Issues (Linux)**:
```bash
# Add user to docker group
sudo usermod -aG docker $USER
# Restart terminal or run:
newgrp docker
```

**AWS CLI Not Found in Git Bash**:
- Use the Python-based approach instead
- Or install AWS CLI v2 for Windows from AWS website

**File Path Issues**:
- Ensure you're in the correct project directory
- Check that `data/reviews_devset.json` exists
- Use absolute paths if relative paths fail

**LocalStack Connection Issues**:
```bash
# Check if LocalStack is running
docker ps | grep localstack

# Restart LocalStack
docker stop localstack
docker run --rm -d --name localstack -p 4566:4566 -e SERVICES=s3,lambda,events,iam localstack/localstack
```

**Python Module Not Found**:
```bash
# Ensure virtual environment is activated
# Windows:
.venv\Scripts\Activate.ps1
# Linux/macOS:  
source .venv/bin/activate

# Install missing modules
pip install requests boto3 nltk profanityfilter
```

## Tips & Tricks Compliance

✅ **Implemented Recommendations:**
- Uses `profanityfilter` package for profanity detection
- Uses NLTK for text preprocessing  
- Includes EventBridge setup for multiple function triggers
- Proper Lambda packaging with dependencies
- NLTK data pre-download for runtime availability
- LocalStack integration commands

✅ **Package Management:**
- Each function has separate requirements.txt
- Deployment script handles dependency packaging
- Stays under 250MB Lambda size limit