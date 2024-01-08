#!/bin/sh
echo "Running as user: $(whoami)"

# Determine the appropriate command to use for python
if command -v python3 > /dev/null 2>&1
then
    PYTHON_CMD="python3"
    echo "Using python3"
elif command -v python > /dev/null 2>&1
then
    PYTHON_CMD="python"
    echo "Using python"
else
    echo "Could not find a suitable python command"
    exit 1
fi

# Extract pack name from properties.yaml using Python
echo "Extracting pack name..."
# $PYTHON_CMD -m pip install pyyaml
# PACK_NAME=$($PYTHON_CMD get_pack_name.py)
PACK_NAME=$(grep 'name:' properties.yaml | awk '{print $2}')
if [ $? -ne 0 ]; then
    echo "Failed to extract pack name."
    exit 1
fi
echo "Pack name: $PACK_NAME"

# Install poetry if it's not installed
if ! command -v poetry > /dev/null
then
    echo "Poetry could not be found, installing now..."
    export POETRY_HOME="$HOME/.poetry"
    curl -sSL https://install.python-poetry.org | $PYTHON_CMD -
    export PATH="$HOME/.poetry/bin:$PATH"
    if [ $? -ne 0 ]; then
        echo "Failed to install Poetry."
        exit 1
    fi
fi

# Get Python version
PYTHON_VERSION=$($PYTHON_CMD -V | cut -d " " -f 2 | cut -d "." -f1,2)
echo "Detected Python version: $PYTHON_VERSION"

# Attempt to install python3-venv if not present
if ! $PYTHON_CMD -m venv --help > /dev/null 2>&1; then
    echo "python3-venv is not installed. Attempting to install now..."
    sudo apt update
    sudo apt install "python${PYTHON_VERSION}-venv" -y
    if [ $? -ne 0 ]; then
        echo "Failed to install python3-venv for Python $PYTHON_VERSION."
        exit 1
    fi
fi

# Check if virtual environment specific to the pack exists in the parent directory
VENV_PATH="$HOME/.qalita/agent_run_temp/${PACK_NAME}_venv"
echo "Virtual Environment Path: $VENV_PATH"

if [ ! -d "$VENV_PATH" ]; then
    echo "Creating virtual environment..."
    /usr/bin/python3 -m venv "$VENV_PATH"  # Ensure using system Python
    if [ $? -ne 0 ]; then
        echo "Failed to create virtual environment for $PACK_NAME."
        exit 1
    fi
    echo "Virtual environment created."
else
    echo "Virtual environment already exists."
fi

# Check if activate script is executable
if [ ! -x "$VENV_PATH/bin/activate" ]; then
    echo "Setting execute permissions on activate script..."
    chmod +x "$VENV_PATH/bin/activate"
    if [ $? -ne 0 ]; then
        echo "Failed to set execute permissions on activate script."
        exit 1
    fi
fi

# Activate the virtual environment specific to the pack from the parent directory
echo "Activating virtual environment..."
. "$VENV_PATH/bin/activate"
if [ $? -ne 0 ]; then
    echo "Failed to activate virtual environment."
    exit 1
fi
echo "Virtual environment activated."

# Install the requirements using poetry
echo "Installing requirements using poetry..."
poetry install --no-root
if [ $? -ne 0 ]; then
    echo "Failed to install requirements."
    exit 1
fi
echo "Requirements installed."

# Run your script
echo "Running script..."
$PYTHON_CMD main.py
if [ $? -ne 0 ]; then
    echo "Script execution failed."
    exit 1
fi
echo "Script executed successfully."

# Deactivate virtual environment
echo "Deactivating virtual environment..."
deactivate
echo "Virtual environment deactivated."
