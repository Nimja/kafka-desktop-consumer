#!/bin/bash

# Get directory of current file.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Make sure we are in this directory.
cd $DIR

# Ensure poetry is installed.
if ! [ -x "$(command -v poetry)" ]; then
    echo "Installing poetry, our package manager."
    pip install 'poetry==1.5.1'
fi

# Create/update environment.
poetry install

# Run app
poetry run python main.py $@
