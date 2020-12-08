#!/bin/bash

# Get directory of current file.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Make sure we are in this directory.
cd $DIR

# Create Virtual env if not existing.
if [ ! -d "./venv" ]
then
    echo "Virtual env does not exist yet, creating..."
    python3 -m venv venv
    ./venv/bin/pip install -r requirements.txt
fi

# Run app
./venv/bin/python main.py
