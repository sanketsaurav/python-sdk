#!/bin/bash

DIR=$(dirname "${BASH_SOURCE[0]}")
python3 -m venv cli
. cli/bin/activate

pushd $DIR
pip install -U pip
pip install -r requirements.txt 
pip install -e .
popd


