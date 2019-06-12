#!/bin/bash

# Set up the local python 3.6 virtual env. 

# Get the path to parent directory of this script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
cd $DIR # Go to the project top level dir.

# Deactivate any current python virtual environment we may be running.
if ! [ -z "${VIRTUAL_ENV}" ] ; then
    deactivate
fi

# Remove any existing python virtual env and rebuild it for py 3.6.
rm -fr $DIR/pyenv
python3.6 -m venv $DIR/pyenv
source $DIR/pyenv/bin/activate

# Install the python modules we need (plus any dependencies).
pip3 install --requirement $DIR/requirements.txt

deactivate

