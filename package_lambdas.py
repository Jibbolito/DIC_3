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

# Constants
PYTHON_VERSION = '3.10'  # Adjust as needed for your Lambda runtime

def package_lambda(function_name, function_dir):
    """Package a Lambda function with its dependencies"""
    print(f"  Packaging {function_name}...")
    
    # Create temporary directory with shorter path for Windows compatibility
    if os.name == 'nt':  # Windows
        temp_base = 'C:\\temp'
        os.makedirs(temp_base, exist_ok=True)
        temp_dir = tempfile.mkdtemp(dir=temp_base, prefix='lambda_')
    else:
        temp_dir = tempfile.mkdtemp()
    
    try:
        # Copy function code
        # Adjusted to match README.md: src/lambda_functions/<function_dir>/lambda_function.py
        function_path = os.path.join('src', 'lambda_functions', function_dir) 
        temp_function_path = os.path.join(temp_dir, 'function')
        
        # Ensure the source directory exists
        if not os.path.exists(function_path):
            print(f"     Error: Source directory '{function_path}' not found for {function_name}.")
            return None

        shutil.copytree(function_path, temp_function_path)
        
        # Install dependencies if requirements.txt exists
        requirements_file = os.path.join(temp_function_path, 'requirements.txt')
        if os.path.exists(requirements_file):
            print(f"     Installing dependencies for {function_name} (forcing source build for compatibility)...")
            # Set up pip environment with shorter temp directory for Windows
            pip_env = os.environ.copy()
            if os.name == 'nt':  # Windows
                pip_temp = 'C:\\temp\\pip'
                os.makedirs(pip_temp, exist_ok=True)
                pip_env['TMPDIR'] = pip_temp
                pip_env['TMP'] = pip_temp
                pip_env['TEMP'] = pip_temp
            
            # Force Linux platform packages for Lambda compatibility
            print(f"     Installing Linux packages for Lambda...")
            try:
                # Force Linux packages - this is critical for Lambda deployment
                subprocess.run([
                    sys.executable, '-m', 'pip', 'install',
                    '--platform', 'linux_x86_64',
                    '--only-binary=:all:',
                    '--implementation', 'cp',
                    '--python-version', PYTHON_VERSION,
                    '--abi', 'cp310m',
                    '-r', requirements_file,
                    '-t', temp_function_path,
                    '--quiet',
                    '--upgrade',
                    '--force-reinstall'
                ], check=True, env=pip_env)
                print(f"     Linux packages installed successfully!")
            except subprocess.CalledProcessError:
                print(f"     Linux package install failed, trying individual packages...")
                # Read requirements and install each package individually
                with open(requirements_file, 'r') as f:
                    packages = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                
                for package in packages:
                    try:
                        print(f"     Installing {package} for Linux...")
                        subprocess.run([
                            sys.executable, '-m', 'pip', 'install',
                            '--platform', 'linux_x86_64',
                            '--only-binary=:all:',
                            '--implementation', 'cp',
                            '--python-version', PYTHON_VERSION,
                            '--abi', 'cp310m',
                            package,
                            '-t', temp_function_path,
                            '--quiet',
                            '--upgrade',
                            '--force-reinstall'
                        ], check=True, env=pip_env)
                    except subprocess.CalledProcessError:
                        print(f"     Warning: Could not install {package} with Linux binaries, trying no-binary...")
                        subprocess.run([
                            sys.executable, '-m', 'pip', 'install',
                            '--platform', 'linux_x86_64',
                            '--no-binary', package,
                            '--implementation', 'cp',
                            '--python-version', PYTHON_VERSION,
                            package,
                            '-t', temp_function_path,
                            '--quiet',
                            '--no-deps'
                        ], check=True, env=pip_env)

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
    
    finally:
        # Clean up temporary directory on Windows
        if os.name == 'nt':
            shutil.rmtree(temp_dir, ignore_errors=True)

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