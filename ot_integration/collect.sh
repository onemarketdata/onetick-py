#!/bin/bash

mkdir -p ./packages
python3 -m venv dev_env
source ./dev_env/bin/activate
python3 -m pip install -r ../requirements.dev.txt
python3 -m pip install onetick-py-test

LIBS_PATH=./dev_env/lib64/python3.9/site-packages

# dist packages
mkdir -p ./dist_pkgs/onetick/py
mkdir -p ./dist_pkgs/onetick/lib
cp -r $LIBS_PATH/locator_parser ./dist_pkgs/
cp -r $LIBS_PATH/onetick/lib ./dist_pkgs/onetick
cp -r ../onetick/py ./dist_pkgs/onetick
cp -r ../onetick/__init__.py ./dist_pkgs/onetick

# packages for testing
mkdir -p ./3rd_party_pkgs
mkdir -p ./3rd_party_pkgs/onetick/test
cp -r $LIBS_PATH/coolname ./3rd_party_pkgs/
cp -r $LIBS_PATH/dateutil ./3rd_party_pkgs/
cp -r $LIBS_PATH/numpy ./3rd_party_pkgs/
cp -r $LIBS_PATH/numpy.libs ./3rd_party_pkgs/
cp -r $LIBS_PATH/pandas ./3rd_party_pkgs/
cp -r $LIBS_PATH/pluggy ./3rd_party_pkgs/
cp -r $LIBS_PATH/py ./3rd_party_pkgs/
cp -r $LIBS_PATH/pytest.py ./3rd_party_pkgs/
cp -r $LIBS_PATH/_pytest ./3rd_party_pkgs/
cp -r $LIBS_PATH/pytz ./3rd_party_pkgs/
cp -r $LIBS_PATH/six.py ./3rd_party_pkgs/
cp -r $LIBS_PATH/attr ./3rd_party_pkgs/
cp -r $LIBS_PATH/attrs ./3rd_party_pkgs/
cp -r $LIBS_PATH/packaging ./3rd_party_pkgs/
cp -r $LIBS_PATH/more_itertools ./3rd_party_pkgs/
cp -r $LIBS_PATH/onetick/test ./3rd_party_pkgs/onetick

# resources
cp -r ../doctest_resources .

# remove __pycache__ and compiled files
find . | grep -E "(/__pycache__$|\.pyc$|\.pyo$)" | xargs rm -rf

# test
deactivate

python3 -m venv empty_env
source ./empty_env/bin/activate

export BUILD_PATH=/opt

export PATH=$PATH:${BUILD_PATH}/one_market_data/one_tick/bin

export PYTHONPATH=${BUILD_PATH}/one_market_data/one_tick/bin:$PYTHONPATH
export PYTHONPATH=$PYTHONPATH:${BUILD_PATH}/one_market_data/one_tick/bin/python
export PYTHONPATH=$PYTHONPATH:${BUILD_PATH}/one_market_data/one_tick/bin/numpy/python39
export PYTHONPATH=$(pwd)/3rd_party_pkgs/:$PYTHONPATH
export PYTHONPATH=$(pwd)/dist_pkgs/:$PYTHONPATH

python3 -sB -m pytest --confcutdir=./dist_pkgs/ --doctest-modules -o doctest_optionflags=NORMALIZE_WHITESPACE ./dist_pkgs/onetick/py

deactivate

# pack
find . | grep -E "(/__pycache__$|\.pyc$|\.pyo$)" | xargs rm -rf

mkdir -p ./packages
zip -r ./packages/3rd_party_pkgs.zip ./3rd_party_pkgs
zip -r ./packages/dist_pkgs.zip ./dist_pkgs
zip -r ./packages/doctest_resources.zip ./doctest_resources

# documentation
source ./dev_env/bin/activate

cd ../doc_sphinx/
./build.sh
cd _build
zip -r ../../ot_integration/packages/html.zip ./html

cd ../../ot_integration

deactivate

# copy artifcats
cp ./artifacts/* ./packages/

rm -rf ./dev_env
rm -rf ./empty_env
rm -rf ./doctest_resources
rm -rf ./3rd_party_pkgs
rm -rf ./dist_pkgs
