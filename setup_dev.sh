#!/bin/bash

DIR=$(dirname "${BASH_SOURCE[0]}")
virtualenv cli
. cli/bin/activate

pushd $DIR
pip install -U pip
pip install -r requirements.txt 
pip install -e .
popd


