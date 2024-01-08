#!/bin/sh
echo "Running as user: $(whoami)"

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

export POETRY_INSTALLER_MAX_WORKERS=10
export POETRY_CACHE_DIR="$HOME/.qalita/agent_run_temp/"
export POETRY_VIRTUALENVS_PATH="$HOME/.qalita/agent_run_temp/${PACK_NAME}_venv"

# Install poetry if it's not installed
if ! command -v poetry > /dev/null
then
    echo "Poetry could not be found, installing now..."
    export POETRY_HOME="$HOME/.poetry"
    curl -sSL https://install.python-poetry.org | $PYTHON_CMD -
    export PATH="$HOME/.poetry/bin:$PATH"
fi

# Attempt to install python3-venv if not present
if ! $PYTHON_CMD -m venv --help > /dev/null 2>&1; then
    echo "python3-venv is not installed. Attempting to install now..."
    sudo apt update
    sudo apt install python3-venv -y
fi

# Identify the Poetry environment path
POETRY_VENV_PATH=$(poetry env info -p)

# Activate the Poetry virtual environment
. "$POETRY_VENV_PATH/bin/activate"

# Install the requirements using poetry
poetry install --no-root

# Run your script
$PYTHON_CMD main.py

# Deactivate virtual environment
deactivate
