#!/bin/bash

set -eu

TAGS=${TAGS:-Nothing}
TARGET=${1:-'html'}
FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
WARNINGS_FLAG='-W'
if [ "$TARGET" == "markdown" ]; then
    WARNINGS_FLAG=''
fi

# make package available for jupyter notebook docs
export PYTHONPATH=${FILE_DIR}/../src:${PYTHONPATH:-""}

# to make session and resources available in notebooks
export RESOURCES_DIR=${FILE_DIR}/notebooks_resources
export ONE_TICK_CONFIG=${FILE_DIR}/notebooks_resources/config/main

export OTP_DEFAULT_DB="DEMO_L1"
export OTP_DEFAULT_SYMBOL="AAPL"
export OTP_DEFAULT_START_TIME="2003/12/01 00:00:00"
export OTP_DEFAULT_END_TIME="2003/12/04 00:00:00"
export OTP_DEFAULT_TZ="EST5EDT"

echo "generate js/switcher.json for multiple versions of docs (works only when serving to root URL)"
python make_switcher_js.py

echo "Cleaning build directory..."
make clean

echo "Building sphinx with $TARGET target..."
make $TARGET SPHINXOPTS="-T $WARNINGS_FLAG --keep-going -t $TAGS"
if [ "$TARGET" == "html" ]; then
    make spelling SPHINXOPTS="-T $WARNINGS_FLAG --keep-going"
fi

rm js/switcher.json
