#!/bin/sh

# Determine the appropriate command to use for python
if command -v python3 > /dev/null 2>&1
then
    PYTHON_CMD="python3"
elif command -v python > /dev/null 2>&1
then
    PYTHON_CMD="python"
else
    echo "Could not find a suitable python command"
    exit 1
fi

# Extract pack name from properties.yaml using Python
PACK_NAME=$($PYTHON_CMD get_pack_name.py)

# Install poetry if it's not installed
if ! command -v poetry &> /dev/null
then
    echo "Poetry could not be found, installing now..."
    curl -sSL https://install.python-poetry.org | $PYTHON_CMD -
    export PATH="/root/.local/bin:$PATH"
fi

# Attempt to install python3-venv if not present
if ! $PYTHON_CMD -m venv --help > /dev/null 2>&1; then
    echo "python3-venv is not installed. Attempting to install now..."
    sudo apt update
    sudo apt install python3-venv -y
fi

# Check if virtual environment specific to the pack exists in the parent directory
if [ ! -d "../../${PACK_NAME}_venv" ]; then
    # If not, create it
    $PYTHON_CMD -m venv ../../${PACK_NAME}_venv
    if [ $? -ne 0 ]; then
        echo "Failed to create virtual environment for $PACK_NAME."
        exit 1
    fi
fi

# Activate the virtual environment specific to the pack from the parent directory
. ../../${PACK_NAME}_venv/bin/activate

# Install the requirements using poetry
poetry install --no-root

# Run your script
$PYTHON_CMD main.py

# Deactivate virtual environment
deactivate
