#!/bin/bash

CASE=$1

DIFF=$(diff -r $CASE/out $CASE/expected_out)
if [[ $DIFF != "" ]]
then
    echo "FAILURE $CASE: $DIFF"
fi
