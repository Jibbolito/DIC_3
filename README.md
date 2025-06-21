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
├── analyze_devset_simple.py       # Analysis script for results
├── process_devset.py              # Alternative processing script
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

Following the official Environment Setup guide:

### Prerequisites
- **Python 3.11** (3.9+ compatible)
- **Docker** (for LocalStack)
- **Utilities**: jq, curl, zip/tar

### Environment Setup

**Option 1: Automated Setup (Recommended)**
```bash
# Run the complete environment setup
chmod +x setup_environment.sh
./setup_environment.sh
```

**Option 2: Manual Setup**

1. **Python 3.11 with pyenv** (recommended):
```bash
# Install pyenv first (see: https://github.com/pyenv/pyenv)
pyenv install 3.11.6
pyenv local 3.11.6
python --version  # Should show Python 3.11.6
```

2. **Virtual Environment**:
```bash
cd DIC2025_Assignment3
python -m venv .venv

# Activate (choose your platform):
# Linux & macOS:
source .venv/bin/activate
# Windows cmd:
.venv\Scripts\activate.bat
# Windows PowerShell:  
.venv\Scripts\Activate.ps1
```

3. **Install Dependencies**:
```bash
pip install -r requirements.txt
```

4. **Start LocalStack**:
```bash
LOCALSTACK_ACTIVATE_PRO=0 LOCALSTACK_DEBUG=1 localstack start
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

### Run Dataset Analysis
```bash
# Analyze the full dataset (78,829 reviews)
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