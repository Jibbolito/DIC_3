#!/usr/bin/env python3
"""
Package Lambda functions for deployment
"""
import os
import zipfile
import shutil
import tempfile
import subprocess
import sys

PYTHON_VERSION = '3.12' 

def package_lambda(function_name, function_dir):
    """Package a Lambda function with its dependencies"""
    print(f"  Packaging {function_name}...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Copy function code
        function_path = os.path.join('src', 'lambda_functions', function_dir) 
        temp_function_path = os.path.join(temp_dir, 'function')
        
        if not os.path.exists(function_path):
            print(f"     Error: Source directory '{function_path}' not found for {function_name}.")
            return None

        shutil.copytree(function_path, temp_function_path)
        
        # Install dependencies if requirements.txt exists
        requirements_file = os.path.join(temp_function_path, 'requirements.txt')
        if os.path.exists(requirements_file):
            print(f"     Installing dependencies for {function_name} ...")
            subprocess.run([
                sys.executable, '-m', 'pip', 'install',
                '--platform', 'manylinux2014_x86_64',
                '-r', requirements_file,
                '-t', temp_function_path,
                '--quiet',
                '--force-reinstall', 
                '--no-cache-dir',
                '--only-binary=:all:',
                '--upgrade',
            ], check=True)

            print(f"     Dependencies for {function_name} installed.")
        else:
            print(f"     No requirements.txt found for {function_name}. Skipping dependency installation.")

        # Create deployment package
        package_name = f"{function_name}_deployment.zip"
        package_path = os.path.join('deployments', package_name)
        
        # Ensure deployments directory exists
        os.makedirs('deployments', exist_ok=True)
        
        # Create zip file
        with zipfile.ZipFile(package_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(temp_function_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, temp_function_path)
                    zipf.write(file_path, arcname)
        
        print(f"     Created {package_path}")
        return package_path

def main():
    """Package all Lambda functions"""
    print("  Starting Lambda packaging...")
    
    # Define your Lambda functions and their corresponding directories
    # Assumes your Lambda handler is lambda_function.py inside these directories
    functions_to_package = [
        ('preprocessing', 'preprocessing'),
        ('profanity_check', 'profanity_check'),
        ('sentiment_analysis', 'sentiment_analysis')
    ]
    
    packages = []
    
    for function_name, function_dir in functions_to_package:
        try:
            package_path = package_lambda(function_name, function_dir)
            if package_path:
                packages.append(package_path)
        except Exception as e:
            print(f"  Failed to package {function_name}: {e}")
            # Optionally re-raise or exit here if a single failure should stop the process
    
    if not packages:
        print("\n  No Lambda functions were successfully packaged.")
        return 1

    print(f"\n  Successfully packaged {len(packages)} Lambda functions:")
    for package in packages:
        file_size = os.path.getsize(package) / 1024 / 1024  # MB
        print(f"     {package} ({file_size:.1f} MB)")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())