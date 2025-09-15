#!/bin/bash

# Utility script short-cut that helps to run doc tests in
# static .rst files. 
# It expects the path to .rst fiels. Basic simple example to run is to use `static/`

# short-cut script to run tests in .rst files
pytest --doctest-glob="*.rst" --doctest-modules $@
