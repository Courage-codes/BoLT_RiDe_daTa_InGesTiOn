#!/bin/bash
set -e
LAMBDA_NAME=$1
ZIP_NAME=$2

cd src
pip install -r ../requirements.txt -t . > /dev/null
zip -r9 ../deployment/${ZIP_NAME} ./${LAMBDA_NAME}.py utils.py config.py __init__.py > /dev/null
cd ..
echo "Packaged Lambda as deployment/${ZIP_NAME}"
