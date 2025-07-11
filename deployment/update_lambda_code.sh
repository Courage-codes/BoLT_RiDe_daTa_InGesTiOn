#!/bin/bash
set -e
FUNCTION_NAME=$1
ZIP_FILE=$2

aws lambda update-function-code \
  --function-name $FUNCTION_NAME \
  --zip-file fileb://deployment/$ZIP_FILE
