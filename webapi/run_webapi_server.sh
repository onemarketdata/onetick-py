#!/bin/bash

rm -rf dbs
mkdir -p dbs/otq_cache
python3 create-config.py
ONE_TICK_CONFIG=./one_tick_config_webapi.txt ./one_market_data/one_tick/bin/tick_server.exe -port 48029
# ONE_TICK_CONFIG=./one_tick_config_webapi.txt  /mnt/onetick/20240330-2/one_market_data/one_tick/bin/tick_server.exe -port 48029