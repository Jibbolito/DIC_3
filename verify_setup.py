#!/usr/bin/env python3
"""
Environment Setup Verification Script
Checks all requirements from Environment_Setup.md
"""
import subprocess
import sys
import json
import requests
from packaging import version

def run_command(cmd):
    """Run command and return output"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout.strip(), result.stderr.strip()
    except Exception as e:
        return False, "", str(e)

def check_python_version():
    """Check Python version meets requirements"""
    print("  Checking Python version...")
    
    current_version = sys.version.split()[0]
    print(f"   Found Python {current_version}")
    
    if version.parse(current_version) >= version.parse("3.9.0"):
        if version.parse(current_version) >= version.parse("3.11.0"):
            print("   Python 3.11+ (recommended)")
        else:
            print("   Python 3.9+ (compatible)")
        return True
    else:
        print("   Python 3.9+ required")
        return False

def check_virtualenv():
    """Check if running in virtual environment"""
    print("  Checking virtual environment...")
    
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("   Virtual environment active")
        return True
    else:
        print("   No virtual environment detected (recommended but not required)")
        return True

def check_utility(name, cmd, required=True):
    """Check if utility is available"""
    print(f"  Checking {name}...")
    
    success, output, error = run_command(cmd)
    if success:
        print(f"   {name} is available: {output.split()[0] if output else 'installed'}")
        return True
    else:
        if required:
            print(f"   {name} is required but not found")
        else:
            print(f"   {name} not found (optional)")
        return not required

def check_docker():
    """Check Docker installation and status"""
    print("  Checking Docker...")
    
    # Check if docker command exists
    success, output, error = run_command("docker --version")
    if not success:
        print("   Docker is not installed")
        return False
    
    print(f"   Docker installed: {output}")
    
    # Check if Docker is running
    success, output, error = run_command("docker info")
    if not success:
        print("   Docker is not running")
        print("   Please start Docker Desktop or Docker Engine")
        return False
    
    print("   Docker is running")
    return True

def check_python_packages():
    """Check required Python packages"""
    print("  Checking Python packages...")
    
    required_packages = [
        'boto3', 'nltk', 'profanityfilter', 'localstack',
        'pytest', 'moto', 'requests'
    ]
    
    all_installed = True
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"   {package}")
        except ImportError:
            print(f"   {package} (run: pip install -r requirements.txt)")
            all_installed = False
    
    return all_installed

def check_localstack():
    """Check LocalStack installation and status"""
    print("  Checking LocalStack...")
    
    # Check if localstack command exists
    success, output, error = run_command("localstack --version")
    if not success:
        print("   LocalStack is not installed")
        return False
    
    print(f"   LocalStack installed: {output}")
    
    # Check if LocalStack is running
    try:
        response = requests.get("http://localhost:4566/_localstack/health", timeout=5)
        if response.status_code == 200:
            health = response.json()
            print("   LocalStack is running")
            print("   Available services:")
            for service, status in health.get('services', {}).items():
                status_icon = "PASS" if status == "available" else "FAIL"
                print(f"      {status_icon} {service}: {status}")
            return True
        else:
            print("   LocalStack health check failed")
            return False
    except requests.exceptions.RequestException:
        print("   LocalStack is not running")
        print("   Start with: LOCALSTACK_ACTIVATE_PRO=0 LOCALSTACK_DEBUG=1 localstack start")
        return False

def check_aws_cli_local():
    """Check if awslocal is working"""
    print("  Checking AWS CLI LocalStack integration...")
    
    # Set environment variables for LocalStack
    import os
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    os.environ['AWS_ENDPOINT_URL'] = 'http://localhost:4566'
    
    success, output, error = run_command("awslocal s3 ls 2>/dev/null || aws s3 ls --endpoint-url http://localhost:4566")
    if success:
        print("   AWS CLI working with LocalStack")
        return True
    else:
        print("   AWS CLI not configured for LocalStack (will use boto3 directly)")
        return True

def run_verification():
    """Run complete verification"""
    print("  Environment Setup Verification")
    print("=" * 50)
    
    checks = [
        ("Python Version", check_python_version),
        ("Virtual Environment", check_virtualenv),  
        ("jq utility", lambda: check_utility("jq", "jq --version")),
        ("curl utility", lambda: check_utility("curl", "curl --version")),
        ("zip utility", lambda: check_utility("zip", "zip --version") or check_utility("tar", "tar --version")),
        ("Docker", check_docker),
        ("Python Packages", check_python_packages),
        ("LocalStack", check_localstack),
        ("AWS CLI Local", check_aws_cli_local)
    ]
    
    results = []
    
    for name, check_func in checks:
        try:
            result = check_func()
            results.append((name, result))
        except Exception as e:
            print(f"   Error checking {name}: {e}")
            results.append((name, False))
        print()
    
    # Summary
    print("  Verification Summary")
    print("=" * 50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{status} {name}")
    
    print(f"\nOverall: {passed}/{total} checks passed")
    
    if passed == total:
        print("  Environment setup is complete and ready!")
        print("\n  Next steps:")
        print("   1. Run tests: python run_tests.py")
        print("   2. Deploy functions: ./package_lambdas.py")
        print("   3. Setup EventBridge: ./setup_eventbridge.sh")
        return True
    else:
        print("  Some checks failed. Please review the issues above.")
        return False

if __name__ == '__main__':
    success = run_verification()
    sys.exit(0 if success else 1)