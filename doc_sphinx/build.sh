#!/bin/bash

set -eux

TAGS=${TAGS:-Nothing}
TARGET=${1:-'html'}
FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
WARNINGS_FLAG=${WARNINGS_FLAG:-'-W --keep-going'}
if [ "$TARGET" == "markdown" ]; then
    WARNINGS_FLAG=''
fi
SPHINXOPTS=${SPHINXOPTS:-"-T -t $TAGS"}

cd $FILE_DIR

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
export OTP_DEFAULT_LICENSE_DIR="${OTP_DEFAULT_LICENSE_DIR:-/license/}"
export OTP_DEFAULT_LICENSE_FILE="${OTP_DEFAULT_LICENSE_FILE:-/license/license.dat}"

echo "generate js/switcher.json for multiple versions of docs (works only when serving to root URL)"
python make_switcher_js.py

echo "Cleaning build directory..."
make clean

echo "Building sphinx with $TARGET target..."
make $TARGET SPHINXOPTS="$SPHINXOPTS $WARNINGS_FLAG"
if [ "$TARGET" == "html" ]; then
    echo "Building sphinx with spelling target..."
    # TODO: seems like both spelling and markdown targets have malformed results
    # if they are built after html in the same directory
    # using separate directory to avoid these issues
    make clean BUILDDIR=_build_other
    make spelling SPHINXOPTS="$SPHINXOPTS $WARNINGS_FLAG" BUILDDIR=_build_other
    # TODO: disabling warnings for markdown for now, we have some problems
    make markdown SPHINXOPTS="$SPHINXOPTS" BUILDDIR=_build_other
    cd _build_other/markdown
    # concatenate all markdown files into llms-full.txt file and move it to html docs source
    find -type f -name '*.md' -exec sh -c 'file_path="${1#./}";\
                                           echo;\
                                           echo ----------------------------------------------------------------------------------;\
                                           echo source: \"https://docs.pip.distribution.sol.onetick.com/${file_path%.md}.html.md\";\
                                           echo ----------------------------------------------------------------------------------;\
                                           echo;\
                                           cat $1' sh {} \; > ../../_build/html/llms-full.txt
    # move all markdown files to _build/html directory, we want to publish them too
    # also rename them so they have .html.md extension so we can add .md suffix for each html page
    find -type f -name '*.md' -exec sh -c 'mv -v $1 ../../_build/html/${1%.md}.html.md' sh {} \;
    cd -
fi

rm js/switcher.json
