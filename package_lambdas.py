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

def package_lambda(function_name, function_dir):
    """Package a Lambda function with its dependencies"""
    print(f"📦 Packaging {function_name}...")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Copy function code
        function_path = os.path.join('src', 'lambda_functions', function_dir)
        temp_function_path = os.path.join(temp_dir, 'function')
        shutil.copytree(function_path, temp_function_path)
        
        # Install dependencies if requirements.txt exists
        requirements_file = os.path.join(temp_function_path, 'requirements.txt')
        if os.path.exists(requirements_file):
            print(f"   📥 Installing dependencies for {function_name}...")
            subprocess.run([
                sys.executable, '-m', 'pip', 'install',
                '-r', requirements_file,
                '-t', temp_function_path,
                '--quiet'
            ], check=True)
        
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
        
        print(f"   ✅ Created {package_path}")
        return package_path

def main():
    """Package all Lambda functions"""
    print("🚀 Starting Lambda packaging...")
    
    functions = [
        ('preprocessing', 'preprocessing'),
        ('profanity_check', 'profanity_check')
    ]
    
    packages = []
    
    for function_name, function_dir in functions:
        try:
            package_path = package_lambda(function_name, function_dir)
            packages.append(package_path)
        except Exception as e:
            print(f"❌ Failed to package {function_name}: {e}")
            return 1
    
    print(f"\n✅ Successfully packaged {len(packages)} Lambda functions:")
    for package in packages:
        file_size = os.path.getsize(package) / 1024 / 1024  # MB
        print(f"   📦 {package} ({file_size:.1f} MB)")
    
    print("\n📝 Next steps:")
    print("   1. Upload packages to AWS Lambda")
    print("   2. Update function code using AWS CLI or console")
    print("   3. Test functions with sample data")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())