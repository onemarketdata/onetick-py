
Overview
========

The `onetick-init` repository is an initializer for `onetick` python namespace package.

It is intended to be used as a git submodule of other git repositories
with python packages that want to be accessed with **onetick.** prefix.
For example, [onetick.py](https://gitlab.sol.onetick.com/solutions/py-onetick/onetick-py).

This project only maintains file `onetick/__init__.py`, that is used to provide
logic for finding all `onetick` namespace sub-packages.
This file must be identical in all sub-packages and therefore needs to be shared.


Installation
============

Add this repository as a git submodule in your project
and add a symbolic link to this project's `onetick/__init__.py` file:

    git submodule add -b master ../onetick-init.git onetick-init
    cd onetick
    ln -s ../onetick-init/onetick/__init__.py __init__.py

Also note that the path to the submodule is relative from your project.
Check Gitlab project and group structure and change that path accordingly.
