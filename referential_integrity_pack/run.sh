#!/bin/sh
echo "Running as user: $(whoami)"

if command -v python3 > /dev/null 2>&1; then PYTHON_CMD="python3"; else PYTHON_CMD="python"; fi

PACK_NAME=$(grep 'name:' properties.yaml | awk '{print $2}')
VENV_PATH="$HOME/.qalita/agent_run_temp/${PACK_NAME}_venv"

if ! command -v poetry > /dev/null; then
  export POETRY_HOME="$HOME/.poetry"
  curl -sSL https://install.python-poetry.org | $PYTHON_CMD -
  export PATH="$HOME/.poetry/bin:$PATH"
fi

if ! $PYTHON_CMD -m venv --help > /dev/null 2>&1; then
  sudo apt update && sudo apt install "python$($PYTHON_CMD -V | awk '{print $2}' | cut -d'.' -f1,2)-venv" -y
fi

[ -d "$VENV_PATH" ] || /usr/bin/python3 -m venv "$VENV_PATH"
. "$VENV_PATH/bin/activate"
poetry install --no-root || exit 1
$PYTHON_CMD main.py || exit 1
deactivate


