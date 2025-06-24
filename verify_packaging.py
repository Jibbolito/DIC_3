#!/usr/bin/env python3
"""
Verify that Lambda packaging will work
"""
import os
import sys

def check_requirements():
    """Check if all required packages for packaging are available"""
    required_packages = [
        'boto3',
        'nltk', 
        'profanityfilter',
        'regex'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} - MISSING")
            missing_packages.append(package)
    
    return len(missing_packages) == 0

def check_lambda_structure():
    """Check if Lambda function directories exist"""
    required_dirs = [
        'src/lambda_functions/preprocessing',
        'src/lambda_functions/profanity_check', 
        'src/lambda_functions/sentiment_analysis'
    ]
    
    missing_dirs = []
    
    for dir_path in required_dirs:
        if os.path.exists(dir_path):
            # Check for lambda_function.py
            lambda_file = os.path.join(dir_path, 'lambda_function.py')
            requirements_file = os.path.join(dir_path, 'requirements.txt')
            
            if os.path.exists(lambda_file):
                print(f"✓ {dir_path}/lambda_function.py")
            else:
                print(f"✗ {dir_path}/lambda_function.py - MISSING")
                missing_dirs.append(dir_path)
                
            if os.path.exists(requirements_file):
                print(f"✓ {dir_path}/requirements.txt")
            else:
                print(f"✗ {dir_path}/requirements.txt - MISSING")
        else:
            print(f"✗ {dir_path} - DIRECTORY MISSING")
            missing_dirs.append(dir_path)
    
    return len(missing_dirs) == 0

def main():
    """Main verification function"""
    print("Verifying Lambda packaging requirements...\n")
    
    print("1. Checking Python packages:")
    packages_ok = check_requirements()
    
    print("\n2. Checking Lambda function structure:")
    structure_ok = check_lambda_structure()
    
    print("\n3. Checking packaging script:")
    if os.path.exists('package_lambdas.py'):
        print("✓ package_lambdas.py exists")
        script_ok = True
    else:
        print("✗ package_lambdas.py - MISSING")
        script_ok = False
    
    print(f"\n{'='*50}")
    if packages_ok and structure_ok and script_ok:
        print("✓ ALL CHECKS PASSED - Ready for Lambda packaging!")
        print("\nRun: python package_lambdas.py")
        return 0
    else:
        print("✗ SOME CHECKS FAILED")
        if not packages_ok:
            print("- Install missing packages with: python install_requirements.py")
        if not structure_ok:
            print("- Check Lambda function directory structure")
        if not script_ok:
            print("- Ensure package_lambdas.py exists")
        return 1

if __name__ == '__main__':
    sys.exit(main())