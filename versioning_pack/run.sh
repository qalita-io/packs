#!/bin/sh
# Detect platform
OS_NAME="$(uname 2>/dev/null || echo Windows)"

echo "Running as user: $(whoami)"

# Extract pack name from properties.yaml using Python
echo "Extracting pack name..."
PACK_NAME=$(grep 'name:' properties.yaml | awk '{print $2}')
if [ $? -ne 0 ]; then
    echo "Failed to extract pack name."
    exit 1
fi
echo "Pack name: $PACK_NAME"

# Select a Python interpreter that satisfies the project's pyproject.toml requirement
echo "Resolving Python version from pyproject.toml..."
if [ ! -f pyproject.toml ]; then
    echo "pyproject.toml not found."
    exit 1
fi

REQUIRED_SPEC=$(grep -E '^\s*python\s*=' pyproject.toml | head -n1 | cut -d '=' -f2- | tr -d ' "')
if [ -z "$REQUIRED_SPEC" ]; then
    echo "Could not read python requirement from pyproject.toml."
    exit 1
fi
echo "Python requirement: $REQUIRED_SPEC"

# Extract lower (>=) and upper (<) bounds if present (major.minor only)
MIN_VER=$(echo "$REQUIRED_SPEC" | sed -n 's/.*>=\([0-9]\+\.[0-9]\+\).*/\1/p')
MAX_VER=$(echo "$REQUIRED_SPEC" | sed -n 's/.*<\([0-9]\+\.[0-9]\+\).*/\1/p')

# Version comparison helpers using sort -V
version_ge() {
    # $1 >= $2 ?
    [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -n1)" = "$1" ]
}

version_lt() {
    # $1 < $2 ?
    [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n1)" = "$1" ] && [ "$1" != "$2" ]
}

# Build candidate list of python executables to try (prefer specific versions)
CANDIDATE_CMDS=""
for MINOR in $(seq 6 20); do
    if command -v "python3.$MINOR" > /dev/null 2>&1; then
        CANDIDATE_CMDS="$CANDIDATE_CMDS python3.$MINOR"
    fi
done
if command -v python3 > /dev/null 2>&1; then
    CANDIDATE_CMDS="$CANDIDATE_CMDS python3"
fi
if command -v python > /dev/null 2>&1; then
    CANDIDATE_CMDS="$CANDIDATE_CMDS python"
fi

BEST_CMD=""
BEST_VER=""
for CMD in $CANDIDATE_CMDS; do
    CMD_PATH=$(command -v "$CMD" 2>/dev/null) || continue
    # Filter out Windows .exe if running under Unix sh
    echo "$CMD_PATH" | grep -qE '\\.exe$' && continue
    VER=$("$CMD_PATH" -V 2>&1 | awk '{print $2}' | cut -d'.' -f1,2)
    case "$VER" in
        3.*) : ;;
        *) continue ;;
    esac
    if [ -n "$MIN_VER" ] && ! version_ge "$VER" "$MIN_VER"; then
        continue
    fi
    if [ -n "$MAX_VER" ] && ! version_lt "$VER" "$MAX_VER"; then
        continue
    fi
    if [ -z "$BEST_VER" ] || version_lt "$BEST_VER" "$VER"; then
        BEST_VER="$VER"
        BEST_CMD="$CMD_PATH"
    fi
done

if [ -z "$BEST_CMD" ]; then
    echo "No available Python interpreter satisfies requirement: $REQUIRED_SPEC"
    echo "Available candidates attempted: $CANDIDATE_CMDS"
    exit 1
fi

PYTHON_CMD="$BEST_CMD"
PYTHON_VERSION=$($PYTHON_CMD -V | awk '{print $2}' | cut -d'.' -f1,2)
echo "Selected Python: $PYTHON_CMD (version $PYTHON_VERSION)"

# Install poetry if it's not installed and resolve its binary path
POETRY_BIN="$(command -v poetry 2>/dev/null || true)"
if [ -z "$POETRY_BIN" ]; then
    echo "Poetry could not be found, installing now..."
    export POETRY_HOME="${POETRY_HOME:-$HOME/.poetry}"
    curl -sSL https://install.python-poetry.org | "$PYTHON_CMD" -
    if [ $? -ne 0 ]; then
        echo "Failed to install Poetry."
        exit 1
    fi
    POETRY_BIN="$POETRY_HOME/bin/poetry"
else
    POETRY_BIN="$(command -v poetry)"
fi

# Ensure the export plugin is available (Poetry 2.x)
"$POETRY_BIN" self add poetry-plugin-export >/dev/null 2>&1 || true

echo "Detected Python version: $PYTHON_VERSION"

# Attempt to install python3-venv if not present (Linux only)
if ! $PYTHON_CMD -m venv --help > /dev/null 2>&1; then
    if echo "$OS_NAME" | grep -qi linux; then
        echo "python3-venv is not installed. Attempting to install now..."
        if command -v apt >/dev/null 2>&1; then
            sudo apt update && sudo apt install "python${PYTHON_VERSION}-venv" -y
        elif command -v dnf >/dev/null 2>&1; then
            sudo dnf install "python${PYTHON_VERSION/./}-venv" -y || true
        fi
    fi
    # If still missing, try ensurepip
    "$PYTHON_CMD" -m ensurepip --upgrade >/dev/null 2>&1 || true
fi

# Venv path (Windows uses Scripts, Unix uses bin)
VENV_PATH="$HOME/.qalita/agent_run_temp/${PACK_NAME}_py${PYTHON_VERSION}_venv"
ACTIVATE_SH="$VENV_PATH/bin/activate"
ACTIVATE_PS1="$VENV_PATH/Scripts/Activate.ps1"
ACTIVATE_BAT="$VENV_PATH/Scripts/activate.bat"

echo "Virtual Environment Path: $VENV_PATH"

if [ ! -d "$VENV_PATH" ]; then
    echo "Creating virtual environment..."
    "$PYTHON_CMD" -m venv "$VENV_PATH"
    if [ $? -ne 0 ]; then
        echo "Failed to create virtual environment for $PACK_NAME."
        exit 1
    fi
    echo "Virtual environment created."
else
    echo "Virtual environment already exists."
fi

# Activate venv cross-platform
echo "Activating virtual environment..."
if [ -f "$ACTIVATE_SH" ]; then
    . "$ACTIVATE_SH"
elif [ -f "$ACTIVATE_BAT" ]; then
    # Attempt activation via cmd if running under Git Bash
    if command -v cmd.exe >/dev/null 2>&1; then
        cmd.exe /c "$ACTIVATE_BAT" >/dev/null 2>&1 || true
    fi
elif [ -f "$ACTIVATE_PS1" ]; then
    if command -v powershell >/dev/null 2>&1; then
        powershell -ExecutionPolicy Bypass -File "$ACTIVATE_PS1" >/dev/null 2>&1 || true
    fi
fi

if ! command -v python >/dev/null 2>&1; then
    echo "Failed to activate virtual environment."
    exit 1
fi

echo "Virtual environment activated."
echo "Venv python: $(which python)"
echo "Venv python version: $(python -V 2>&1)"

# Install the requirements using Poetry export + pip (fallback to pip install .)
echo "Installing requirements using poetry..."
export POETRY_VIRTUALENVS_CREATE=false
export PIP_DISABLE_PIP_VERSION_CHECK=1

# Upgrade pip toolchain
python -m pip install --upgrade --quiet pip setuptools wheel

REQ_FILE="$(pwd)/.qalita_requirements.txt"
"$POETRY_BIN" export -f requirements.txt --without-hashes -o "$REQ_FILE" 2>/dev/null
EXPORT_STATUS=$?
if [ $EXPORT_STATUS -eq 0 ] && [ -s "$REQ_FILE" ]; then
    python -m pip install -r "$REQ_FILE" --quiet
    if [ $? -ne 0 ]; then
        echo "Failed to install requirements with pip."
        exit 1
    fi
    echo "Requirements installed from exported lock."
else
    echo "Poetry export unavailable; falling back to installing project with pip."
    python -m pip install .
    if [ $? -ne 0 ]; then
        echo "Failed to install project with pip."
        exit 1
    fi
    echo "Project installed into venv."
fi

# Run your script
echo "Running script..."
python main.py
if [ $? -ne 0 ]; then
    echo "Script execution failed."
    exit 1
fi
echo "Script executed successfully."

# Deactivate virtual environment
echo "Deactivating virtual environment..."
if command -v deactivate >/dev/null 2>&1; then
    deactivate
fi
echo "Virtual environment deactivated."
