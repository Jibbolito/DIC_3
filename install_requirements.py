#!/usr/bin/env python3
"""
Smart requirements installer that handles Windows LocalStack path issues
"""
import os
import sys
import subprocess
import tempfile
import shutil

def install_with_short_paths(packages, temp_base=None):
    """Install packages with shorter temp paths for Windows compatibility"""
    if temp_base is None:
        if os.name == 'nt':  # Windows
            temp_base = 'C:\\temp\\pip_install'
        else:
            temp_base = tempfile.gettempdir()
    
    os.makedirs(temp_base, exist_ok=True)
    
    # Set environment variables for shorter paths
    env = os.environ.copy()
    env['TMPDIR'] = temp_base
    env['TMP'] = temp_base
    env['TEMP'] = temp_base
    env['PIP_CACHE_DIR'] = os.path.join(temp_base, 'cache')
    env['PIP_BUILD_DIR'] = os.path.join(temp_base, 'build')
    
    # Create directories
    os.makedirs(env['PIP_CACHE_DIR'], exist_ok=True)
    os.makedirs(env['PIP_BUILD_DIR'], exist_ok=True)
    
    for package in packages:
        print(f"Installing {package}...")
        try:
            cmd = [sys.executable, '-m', 'pip', 'install', package, '--upgrade']
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"Failed to install {package}: {result.stderr}")
                return False
            else:
                print(f"Successfully installed {package}")
        except Exception as e:
            print(f"Error installing {package}: {e}")
            return False
    
    return True

def main():
    """Install requirements with Windows compatibility"""
    print("Installing requirements with Windows path compatibility...")
    
    # Core packages that usually work fine
    core_packages = [
        'boto3',
        'nltk', 
        'profanityfilter',
        'awscli',
        'awscli-local',
        'pytest',
        'moto',
        'requests',
        'packaging',
        'regex'
    ]
    
    # Problematic packages that need special handling
    localstack_packages = [
        'localstack',
        'localstack-client'
    ]
    
    # Install core packages first
    print("Installing core packages...")
    if not install_with_short_paths(core_packages):
        print("Failed to install core packages")
        return 1
    
    # Skip LocalStack on Windows due to long path issues
    if os.name == 'nt':  # Windows
        print("Skipping LocalStack packages on Windows due to path length limitations")
        print("LocalStack will be used via Docker instead:")
        print("docker run --rm -d --name localstack -p 4566:4566 localstack/localstack")
    else:
        # Try to install LocalStack on Linux/Mac
        print("Installing LocalStack packages...")
        if not install_with_short_paths(localstack_packages):
            print("WARNING: Failed to install LocalStack packages")
            print("You can still use the packaging script, but LocalStack testing may not work")
            return 0
    
    print("\nAll packages installed successfully!")
    return 0

if __name__ == '__main__':
    sys.exit(main())