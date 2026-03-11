#!/bin/bash
# green2blue macOS Installer
# Double-click this file in Finder to install green2blue.
#
# What it does:
#   1. Checks for Python 3.10+ (installs via Homebrew if needed)
#   2. Creates a virtual environment at ~/.green2blue/
#   3. Installs green2blue with encrypted backup support
#   4. Creates a run-green2blue.command launcher on your Desktop
#   5. Launches the interactive wizard

set -e

echo ""
echo "  green2blue Installer"
echo "  ===================="
echo ""

# --- Check for Python ---
PYTHON=""

# Try python3 first
if command -v python3 &>/dev/null; then
    PY_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
    PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)
    if [ "$PY_MAJOR" -ge 3 ] && [ "$PY_MINOR" -ge 10 ]; then
        PYTHON="python3"
    fi
fi

if [ -z "$PYTHON" ]; then
    echo "  Python 3.10+ not found."
    echo ""

    # Try to install via Homebrew
    if command -v brew &>/dev/null; then
        echo "  Installing Python via Homebrew..."
        brew install python@3.12
        PYTHON="python3"
    else
        echo "  Installing Homebrew first..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

        # Add Homebrew to PATH for this session
        if [ -f /opt/homebrew/bin/brew ]; then
            eval "$(/opt/homebrew/bin/brew shellenv)"
        elif [ -f /usr/local/bin/brew ]; then
            eval "$(/usr/local/bin/brew shellenv)"
        fi

        echo "  Installing Python via Homebrew..."
        brew install python@3.12
        PYTHON="python3"
    fi
fi

echo "  Using: $($PYTHON --version)"
echo ""

# --- Create venv ---
VENV_DIR="$HOME/.green2blue"

if [ -d "$VENV_DIR" ]; then
    echo "  Existing installation found at $VENV_DIR"
    echo "  Updating..."
else
    echo "  Creating virtual environment at $VENV_DIR..."
fi

$PYTHON -m venv "$VENV_DIR" --clear
source "$VENV_DIR/bin/activate"

# --- Install green2blue ---
echo "  Installing green2blue..."

# If we're in the repo directory, install from source
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
if [ -f "$SCRIPT_DIR/pyproject.toml" ]; then
    pip install -q "$SCRIPT_DIR[encrypted]"
else
    pip install -q "green2blue[encrypted]"
fi

echo "  Installation complete!"
echo ""

# --- Create Desktop launcher ---
LAUNCHER="$HOME/Desktop/run-green2blue.command"
cat > "$LAUNCHER" << 'LAUNCHER_EOF'
#!/bin/bash
source "$HOME/.green2blue/bin/activate"
green2blue
LAUNCHER_EOF
chmod +x "$LAUNCHER"

echo "  Created launcher: $LAUNCHER"
echo "  Double-click it anytime to run green2blue."
echo ""

# --- Launch wizard ---
echo "  Launching green2blue..."
echo ""
green2blue
