#!/usr/bin/env python3
"""
Fix Lambda packages by downloading Linux wheels directly
"""
import os
import sys
import subprocess
import tempfile
import shutil

def download_linux_wheels():
    """Download precompiled Linux wheels"""
    wheels_dir = "linux_wheels"
    os.makedirs(wheels_dir, exist_ok=True)
    
    # Define packages with their Linux wheel URLs
    packages = [
        "boto3",
        "nltk",
        "textblob", 
        "profanityfilter"
    ]
    
    print("Downloading Linux wheels...")
    for package in packages:
        try:
            subprocess.run([
                sys.executable, '-m', 'pip', 'download',
                '--platform', 'linux_x86_64',
                '--python-version', '310',
                '--abi', 'cp310',
                '--only-binary=:all:',
                '--dest', wheels_dir,
                package
            ], check=True)
            print(f"✓ Downloaded {package}")
        except subprocess.CalledProcessError:
            print(f"⚠ Could not download wheel for {package}, will use source")
    
    # Special handling for regex - download specific version
    try:
        subprocess.run([
            sys.executable, '-m', 'pip', 'download',
            '--platform', 'linux_x86_64',
            '--python-version', '310',
            '--abi', 'cp310',
            '--only-binary=:all:',
            '--dest', wheels_dir,
            'regex==2023.12.25'  # Specific version that should have wheels
        ], check=True)
        print("✓ Downloaded regex wheel")
    except subprocess.CalledProcessError:
        print("⚠ Could not download regex wheel")

def install_from_wheels(target_dir, wheels_dir):
    """Install packages from downloaded wheels"""
    print(f"Installing to {target_dir}...")
    
    # Install all wheels
    wheel_files = [f for f in os.listdir(wheels_dir) if f.endswith('.whl')]
    
    for wheel in wheel_files:
        wheel_path = os.path.join(wheels_dir, wheel)
        try:
            subprocess.run([
                sys.executable, '-m', 'pip', 'install',
                '--target', target_dir,
                '--no-deps',
                wheel_path
            ], check=True)
            print(f"✓ Installed {wheel}")
        except subprocess.CalledProcessError:
            print(f"⚠ Failed to install {wheel}")

def main():
    """Main function"""
    print("Fixing Lambda packages...")
    
    # Download Linux wheels
    download_linux_wheels()
    
    # Create test installation
    test_dir = "test_install"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    os.makedirs(test_dir)
    
    # Install from wheels
    install_from_wheels(test_dir, "linux_wheels")
    
    # Test import
    sys.path.insert(0, test_dir)
    try:
        import regex
        print("✓ regex import successful!")
        print(f"regex version: {regex.__version__}")
    except ImportError as e:
        print(f"✗ regex import failed: {e}")
    
    print("\nNext steps:")
    print("1. Copy the linux_wheels directory to your deployment")
    print("2. Update package_lambdas.py to use these wheels")

if __name__ == '__main__':
    main()