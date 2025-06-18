#!/usr/bin/env python3
"""
Test runner for Lambda functions
"""
import subprocess
import sys
import os

def setup_nltk():
    """Download required NLTK data for tests"""
    try:
        import nltk
        print("Downloading NLTK data...")
        nltk.download('punkt', quiet=True)
        nltk.download('stopwords', quiet=True)
        nltk.download('wordnet', quiet=True)
        nltk.download('averaged_perceptron_tagger', quiet=True)
        print("✅ NLTK data downloaded successfully")
    except Exception as e:
        print(f"⚠️  Warning: Could not download NLTK data: {e}")

def run_tests():
    """Run all tests"""
    print("🚀 Starting test execution...")
    
    # Setup NLTK data
    setup_nltk()
    
    # Change to src directory for relative imports
    test_dir = os.path.join(os.path.dirname(__file__), 'src', 'tests')
    
    # Run pytest with verbose output
    cmd = [
        sys.executable, '-m', 'pytest',
        test_dir,
        '-v',
        '--tb=short',
        '--color=yes'
    ]
    
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=os.path.dirname(__file__))
    
    if result.returncode == 0:
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed!")
        sys.exit(1)

def run_specific_test(test_name):
    """Run a specific test file"""
    test_dir = os.path.join(os.path.dirname(__file__), 'src', 'tests')
    test_file = os.path.join(test_dir, f"test_{test_name}.py")
    
    if not os.path.exists(test_file):
        print(f"❌ Test file {test_file} not found!")
        sys.exit(1)
    
    setup_nltk()
    
    cmd = [
        sys.executable, '-m', 'pytest',
        test_file,
        '-v',
        '--tb=short',
        '--color=yes'
    ]
    
    print(f"Running {test_name} tests...")
    result = subprocess.run(cmd, cwd=os.path.dirname(__file__))
    
    if result.returncode == 0:
        print(f"✅ {test_name} tests passed!")
    else:
        print(f"❌ {test_name} tests failed!")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        if test_name in ['preprocessing', 'profanity_check', 'integration']:
            run_specific_test(test_name)
        else:
            print("Available tests: preprocessing, profanity_check, integration")
            print("Usage: python run_tests.py [test_name]")
            sys.exit(1)
    else:
        run_tests()