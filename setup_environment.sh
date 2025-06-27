#!/bin/bash

# Environment Setup Script following the official Environment Setup guide
# This script automates the setup process described in Environment_Setup.md

set -e

echo "Setting up Assignment 3 Environment..."

# Check Python version
echo "   Checking Python version..."
PYTHON_VERSION=$(python --version 2>&1 | awk '{print $2}')
REQUIRED_MAJOR=3
REQUIRED_MINOR=9

# Extract major and minor version numbers
MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

if [ "$MAJOR" -lt "$REQUIRED_MAJOR" ] || ([ "$MAJOR" -eq "$REQUIRED_MAJOR" ] && [ "$MINOR" -lt "$REQUIRED_MINOR" ]); then
    echo "   Python $REQUIRED_MAJOR.$REQUIRED_MINOR+ required, found $PYTHON_VERSION"
    echo "   Please install Python 3.11+ using pyenv or your preferred method:"
    echo "   - Linux/macOS: https://github.com/pyenv/pyenv"
    echo "   - Windows: https://pyenv-win.github.io/pyenv-win/"
    echo ""
    echo "   Quick pyenv setup:"
    echo "   pyenv install 3.11.6"
    echo "   pyenv local 3.11.6"
    exit 1
fi

echo "   Python $PYTHON_VERSION is compatible"

# Check if we're in a virtual environment
echo "   Checking virtual environment..."
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "   Virtual environment active: $VIRTUAL_ENV"
else
    echo "   No virtual environment detected"
    echo "   It's recommended to create and activate a virtual environment:"
    echo ""
    echo "   python -m venv .venv"
    echo ""
    echo "   # Linux & macOS:"
    echo "   source .venv/bin/activate"
    echo ""
    echo "   # Windows cmd:"
    echo "   .venv\\Scripts\\activate.bat"
    echo ""
    echo "   # Windows PowerShell:"
    echo "   .venv\\Scripts\\Activate.ps1"
    echo ""
    read -p "Continue without virtual environment? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check required utilities
echo "  Checking required utilities..."

# Check jq
if command -v jq &> /dev/null; then
    echo "   jq is installed: $(jq --version)"
else
    echo "   jq is not installed"
    echo "   Installing jq for Windows..."
    
    # Try to install jq for Windows
    if [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]] || [[ -n "$WINDIR" ]]; then
        # Windows environment
        echo "   Downloading jq for Windows..."
        curl -L -o jq.exe https://github.com/jqlang/jq/releases/latest/download/jq-windows-amd64.exe
        
        if [ -f "jq.exe" ]; then
            # Move to a directory in PATH or current directory
            echo "   jq downloaded successfully to current directory"
            # Add current directory to PATH for this session
            export PATH="$PWD:$PATH"
            echo "   jq is now available: $(./jq.exe --version)"
        else
            echo "   Failed to download jq"
            echo "   Please manually download jq from: https://jqlang.org/download/"
            echo "   Or use Windows package manager: winget install jqlang.jq"
            exit 1
        fi
    else
        echo "   Please install jq: https://jqlang.org/download/"
        echo "   - Ubuntu/Debian: sudo apt-get install jq"
        echo "   - macOS: brew install jq"
        echo "   - Windows: winget install jqlang.jq"
        exit 1
    fi
fi

# Check curl
if command -v curl &> /dev/null; then
    echo "   curl is installed: $(curl --version | head -n1)"
else
    echo "   curl is not installed"
    echo "   Please install curl (usually pre-installed on most systems)"
    exit 1
fi

# Check zip (or tar on Unix)
if command -v zip &> /dev/null; then
    echo "   zip is installed: $(zip --version | head -n1)"
elif command -v tar &> /dev/null; then
    # Check if it's GNU tar or BSD tar
    if tar --version &> /dev/null; then
        echo "   tar is available (GNU): $(tar --version | head -n1)"
    else
        echo "   tar is available (BSD/macOS): $(tar --help 2>&1 | head -n1)"
    fi
else
    echo "   Neither zip nor tar is available"
    echo "   Please install zip or ensure tar is available"
    exit 1
fi

# Check Docker
echo "   Checking Docker..."
if command -v docker &> /dev/null; then
    if docker info &> /dev/null; then
        echo "   Docker is installed and running: $(docker --version)"
    else
        echo "   Docker is installed but not running"
        echo "   Please start Docker Desktop or Docker Engine"
        exit 1
    fi
else
    echo "   Docker is not installed"
    echo "   Please install Docker:"
    echo "   - Docker Desktop: https://docs.docker.com/desktop/"
    echo "   - Docker Engine (Linux): https://docs.docker.com/engine/install/"
    exit 1
fi

# Install Python dependencies
echo "   Installing Python dependencies..."

# Use our Windows-compatible installer if available
if [ -f "install_requirements.py" ]; then
    echo "   Using Windows-compatible installer..."
    python install_requirements.py
else
    echo "   Using standard pip installation..."
    pip install -r requirements.txt
fi

echo "   Dependencies installed successfully"

export LOCALSTACK_HEAP_SIZE=2g
export LAMBDA_EXECUTOR=docker-reuse
export LAMBDA_REMOVE_CONTAINERS=false
export LAMBDA_CONTAINER_POOLS=true
export MAIN_CONTAINER_NAME=localstack-main # Important for LAMBDA_CONTAINER_POOLS=true

# Test LocalStack installation
echo "   Testing LocalStack installation..."
if command -v localstack &> /dev/null; then
    echo "   LocalStack is installed: $(localstack --version)"
    
    # Start LocalStack
    echo "   Starting LocalStack..."
    echo "   Using environment variables:"
    echo "   - LOCALSTACK_ACTIVATE_PRO=0 (free tier)"
    echo "   - LOCALSTACK_DEBUG=1 (debug mode)"
    echo ""

    # Check if LocalStack is already running
    if curl -s http://localhost:4566/_localstack/health &> /dev/null; then
        echo "   LocalStack is already running"
    else
        echo "   Starting LocalStack (this may take a few moments)..."
        
        # Start LocalStack in background
        LOCALSTACK_ACTIVATE_PRO=0 LOCALSTACK_DEBUG=1 localstack start &
        LOCALSTACK_PID=$!
        
        # Wait for LocalStack to be ready
        echo "   Waiting for LocalStack to be ready..."
        for i in {1..30}; do
            if curl -s http://localhost:4566/_localstack/health &> /dev/null; then
                echo "   LocalStack is ready!"
                break
            fi
            echo "   Attempt $i/30: LocalStack not ready yet..."
            sleep 2
        done
        
        # Check if LocalStack started successfully
        if ! curl -s http://localhost:4566/_localstack/health &> /dev/null; then
            echo "   LocalStack failed to start"
            echo "   Please check the LocalStack logs and try again"
            kill $LOCALSTACK_PID 2>/dev/null || true
            exit 1
        fi
    fi

    # Verify LocalStack services
    echo "   Checking LocalStack services..."
    HEALTH_RESPONSE=$(curl -s http://localhost:4566/_localstack/health)
    echo "   Health check response:"
    if command -v jq &> /dev/null; then
        echo "$HEALTH_RESPONSE" | jq '.' 2>/dev/null || echo "$HEALTH_RESPONSE"
    elif [ -f "jq.exe" ]; then
        echo "$HEALTH_RESPONSE" | ./jq.exe '.' 2>/dev/null || echo "$HEALTH_RESPONSE"
    else
        echo "$HEALTH_RESPONSE"
    fi
else
    echo "   LocalStack not installed (expected on Windows)"
    echo "   Using Docker-based LocalStack instead..."
    echo "   Starting LocalStack Docker container..."
    
    # Check if LocalStack container is running
    if curl -s http://localhost:4566/_localstack/health &> /dev/null; then
        echo "   LocalStack Docker container is already running"
    else
        echo "   Starting LocalStack Docker container..."
        docker run --rm -d --name localstack -p 4566:4566 -e SERVICES=s3,lambda,events,iam localstack/localstack
        
        # Wait for LocalStack to be ready
        echo "   Waiting for LocalStack Docker container to be ready..."
        for i in {1..30}; do
            if curl -s http://localhost:4566/_localstack/health &> /dev/null; then
                echo "   LocalStack Docker container is ready!"
                break
            fi
            echo "   Attempt $i/30: LocalStack not ready yet..."
            sleep 2
        done
        
        if ! curl -s http://localhost:4566/_localstack/health &> /dev/null; then
            echo "   LocalStack Docker container failed to start"
            echo "   Please check Docker and try again"
            exit 1
        fi
    fi
    
    # Verify LocalStack services
    echo "   Checking LocalStack services..."
    HEALTH_RESPONSE=$(curl -s http://localhost:4566/_localstack/health)
    echo "   Health check response:"
    if command -v jq &> /dev/null; then
        echo "$HEALTH_RESPONSE" | jq '.' 2>/dev/null || echo "$HEALTH_RESPONSE"
    elif [ -f "jq.exe" ]; then
        echo "$HEALTH_RESPONSE" | ./jq.exe '.' 2>/dev/null || echo "$HEALTH_RESPONSE"
    else
        echo "$HEALTH_RESPONSE"
    fi
fi

echo ""
echo "   Environment setup completed successfully!"
echo ""
echo "   Setup Summary:"
echo "      Python $PYTHON_VERSION"
echo "      Required utilities (jq, curl, zip/tar)"
echo "      Docker running"
echo "      Python dependencies installed"
echo "      LocalStack running on http://localhost:4566"
echo ""