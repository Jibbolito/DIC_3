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
    
    # Create temporary directory.
    # On Windows, using a shorter path in C:\temp to avoid MAX_PATH issues
    if os.name == 'nt':
        temp_base = 'C:\\temp'
        os.makedirs(temp_base, exist_ok=True)
        temp_dir = tempfile.mkdtemp(dir=temp_base, prefix='lambda_')
    else:
        temp_dir = tempfile.mkdtemp() # Standard temp directory on Linux/macOS
    
    try:
        # Define the path to the source code for this specific Lambda function
        # This is where your lambda_function.py resides, e.g., src/preprocessing
        function_src_path = os.path.join('src', 'lambda_functions', function_dir) 
        
        # Ensure the source directory exists
        if not os.path.exists(function_src_path):
            print(f"     Error: Source directory '{function_src_path}' not found for {function_name}.")
            return None

        # Copy all contents from the function's source directory directly into temp_dir.
        # This ensures lambda_function.py and requirements.txt are at the root
        # of the temporary directory, and thus at the root of the final zip.
        for item in os.listdir(function_src_path):
            s = os.path.join(function_src_path, item)
            d = os.path.join(temp_dir, item)
            if os.path.isdir(s):
                shutil.copytree(s, d)
            else:
                shutil.copy2(s, d)
        
        # Install dependencies if requirements.txt exists
        requirements_file = os.path.join(temp_dir, 'requirements.txt')
        if os.path.exists(requirements_file):
            print(f"     Installing dependencies for {function_name} (forcing Linux binaries)...")
            
            # Set up pip environment with shorter temp directory for Windows
            pip_env = os.environ.copy()
            if os.name == 'nt':
                pip_temp = 'C:\\temp\\pip'
                os.makedirs(pip_temp, exist_ok=True)
                pip_env['TMPDIR'] = pip_temp
                pip_env['TMP'] = pip_temp
                pip_env['TEMP'] = pip_temp
            
            try:
                # This command forces pip to download and install manylinux wheels
                # compatible with the Lambda execution environment (linux_x86_64).
                # --only-binary=:all: ensures it prefers pre-built binaries.
                # --upgrade and --force-reinstall ensure a clean install.
                subprocess.run([
                    sys.executable, '-m', 'pip', 'install',
                    '--platform', 'linux_x86_64',
                    '--only-binary=:all:',
                    '--implementation', 'cp',
                    '--python-version', PYTHON_VERSION,
                    '--abi', f'cp{PYTHON_VERSION.replace(".", "")}m', # e.g., cp310m for python3.10
                    '-r', requirements_file,
                    '-t', temp_dir, # Install directly into the root of the temporary dir
                    '--quiet',
                    '--upgrade',
                    '--force-reinstall'
                ], check=True, env=pip_env)
                print(f"     Dependencies for {function_name} installed successfully.")
            except subprocess.CalledProcessError as e:
                print(f"     Error installing Linux packages for {function_name}: {e}")
                print("     This might indicate a missing or incompatible dependency.")
                return None
            except Exception as e:
                print(f"     An unexpected error occurred during pip install for {function_name}: {e}")
                return None
        else:
            print(f"     No requirements.txt found for {function_name}. Skipping dependency installation.")

        # Create deployment package
        package_name = f"{function_name}_deployment.zip"
        package_path = os.path.join('deployments', package_name)
        
        # Ensure deployments directory exists
        os.makedirs('deployments', exist_ok=True)
        
        # Create zip file from the contents of the temporary directory.
        # arcname ensures files are placed at the root of the zip.
        with zipfile.ZipFile(package_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # This calculates the path relative to temp_dir,
                    # effectively putting everything at the zip's root.
                    arcname = os.path.relpath(file_path, temp_dir)
                    zipf.write(file_path, arcname)
        
        print(f"     Created {package_path}")
        return package_path
    
    finally:
        # Clean up temporary directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)

def main():
    """Package all Lambda functions"""
    print("  Starting Lambda packaging...")
    
    # Define your Lambda functions and their corresponding directories under 'src'
    # The 'function_dir' here should match the folder name under 'src'.
    # E.g., 'src/preprocessing'
    functions_to_package = [
        ('splitter', 'splitter'),
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
