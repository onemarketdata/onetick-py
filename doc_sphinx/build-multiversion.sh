#!/bin/bash

# THIS SCRIPT WORKS ONLY IN GITLAB CI/CD, DO NOT RUN IT LOCALLY OR ON DEV SERVERS

set -eu
export

# remove 1.55.x version from OneTick build folder in order to fix import errors
# sudo rm -rf /opt/one_market_data/one_tick/bin/python/onetick/py

# # remove latest version from site-packages in order to fix import errors
# sudo pip uninstall -y onetick-py

FILE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "Current folder: $FILE_DIR"
mkdir -p $FILE_DIR/_build/releases

export MAIN_ONE_TICK_DIR=${MAIN_ONE_TICK_DIR:-"/opt/one_market_data/one_tick"}
LOCAL_PIP_URL=${LOCAL_PIP_URL:-"pip.sol.onetick.com"}

# list all tags like x.x.x
TAGS=$(git tag --list | sed 's/ //g' | grep -E '[0-9]+\.[0-9]+\.[0-9]+' | sort -Vr)
echo "Found versions:"
echo "$TAGS"

# for each release branch create a tmp dir, copy this repo to tmp dir, checkout release branch, build docs, copy docs to release branch dir, delete tmp dir
for VERSION in $TAGS
do
    echo "Building docs for OTP version $VERSION"

    # create tmp dir
    TMP_DIR=$(mktemp -d -t sphinx-XXXXXXXXXX)
    echo "Created tmp dir: $TMP_DIR"

    # copy this repo to tmp dir
    cp -r .. $TMP_DIR

    # checkout release branch
    cd $TMP_DIR/doc_sphinx
    git reset --hard
    git checkout $VERSION
    git submodule update --init --recursive

    # if major version is 1 and minor version is below 70, then build docs with PYTHONPATH set
    IFS='.' read -r MAJOR MINOR PATCH <<< "$VERSION"
    if [ "$MAJOR" == "1" ]; then
        if [ "$MINOR" -lt "71" ]; then
            echo "Building docs with PYTHONPATH set for older versions"
            export PYTHONPATH=${TMP_DIR}:${TMP_DIR}/doc_sphinx:/opt/one_market_data/one_tick/bin:/opt/one_market_data/one_tick/bin/python:/opt/one_market_data/one_tick/bin/numpy/python39
        else
            echo "Building docs with PYTHONPATH set only with doc_sphinx and local otp"
            export PYTHONPATH=${TMP_DIR}:${TMP_DIR}/doc_sphinx
        fi
    fi

    REAL_VERSION=$(python -c "import onetick.py as otp; print(otp.__version__)")
    IFS='.' read -r REAL_MAJOR_VERSION REAL_MINOR_VERSION REAL_PATCH_VERSION <<< "$REAL_VERSION"
    # don't checking patch version, because 1.64.0 tag has 1.64.1 version inside
    if [ "$MAJOR" != "$REAL_MAJOR_VERSION" ] || [ "$MINOR" != "$REAL_MINOR_VERSION" ]; then
        echo "ERROR: expected VERSION: $VERSION != imported REAL_VERSION: $REAL_VERSION"
        rm -rf $TMP_DIR
        exit 1
    fi

    # build docs with config from this repo, to preserve all settings (like theme)
    cp $FILE_DIR/conf.py $TMP_DIR/doc_sphinx/conf.py
    ./build.sh html

    # copy docs to release folder
    echo "copying docs to $FILE_DIR/_build/release/$VERSION"
    mkdir -p $FILE_DIR/_build/release/$VERSION
    cp -r ./_build/html/* $FILE_DIR/_build/release/$VERSION

    # delete tmp dir
    cd $FILE_DIR
    rm -rf $TMP_DIR
done
