#!/bin/env python3
"""
Create test configuration for the onetick WebAPI server.
It is intended to run before running `pytest`.
But keep in mind, that this way is deprecated
in favor of OTP_WEBAPI_TEST_MODE usage inside docker-compose containers.
Kept it here as an example of how to create a configuration for the onetick WebAPI server.
"""

import os
current_path = os.path.dirname(os.path.realpath(__file__))
print("current_path:", current_path)

test_dbs = """
AHEAD_DB
COMPATIBLE_DB
DATA
DB
DB_1
DB_2
DB_A
DB_B
DB_INFO
DB_NAME
DB_WITH_SYMBOLS
DB1
DB2
DEFAULT_DB
DELAYED_DB
DECAYED_DB
DIFFERENT_TT_DIFFERENT_DATES_DB
LONG
MANY_TICK_TYPES_DB
MISMATCHED_DB
MQT_DB
MS1
MS2
MY_DB
US_COMP
S_ORDERS_FIX
SOME_DB
SOME_DB_1
SOME_DB_2
TEMP_DB
TEST_AA
TEST_DATAFRAME_LOGIC
TEST_DATES
TEST_DB
TEST_DB_1
TEST_DB_EMPTY
TEST_DB_PRESAVED
TEST_DB_PROPERTY_LOCAL_DB
TEST_DB_PROPERTY_LOCAL_DB_PRE_SAVED
TEST_DB_PROPERTY_LOCAL_DB_PRE_SAVED_EMPTY
TEST_DB_WITH_PARAM
TEST_DEFAULT_START
TEST_NANOSECONDS
TEST_NOTHING
TEST_ORDERS_DB
TEST_PAT
TEST_SIMPLE
TEST_SYMBOL_WITH_COLON
TEST_SYMBOLS
TEST_TT
TESTCACHINGANDCONCURRENCY
TESTDB
TESTDEFAULTDB
TESTSYMBOLPARAM
TESTUNBOUNDANDBOUND
TMP_DB
""".strip().split("\n")

acl_dbs = ""
locator_dbs = ""
for db in test_dbs:
    os.makedirs(f"{current_path}/dbs/{db}", exist_ok=True)
    acl_dbs += f"""
        <DB id="{db}" read_access="true" >
            <ALLOW role="Admin" write_access="true" />
        </DB>
    """

    locator_dbs += f"""
    <DB id="{db}" symbology="BZX" archive_compression_type="NATIVE_PLUS_GZIP" tick_timestamp_type="NANOS" >
        <LOCATIONS>
            <LOCATION access_method="file" start_time="20021230000000"
                end_time="21000101000000" location="{current_path}/dbs/{db}" />
        </LOCATIONS>
        <RAW_DATA>
        </RAW_DATA>
    </DB>
    """

acl = f"""
<roles>
    <role name="Admin">
        <user name="aserechenko" />
        <user name="onetick" />
    </role>
</roles>

<databases>
{acl_dbs}
</databases>

<event_processors>
    <ep ID="RELOAD_CONFIG">
        <allow role="Admin" />
    </ep>

    <ep ID="WRITE_TEXT">
        <allow role="Admin" />
    </ep>

    <ep ID="COMMAND_EXECUTE">
        <allow role="Admin" />
    </ep>

    <ep ID="READ_FROM_RAW">
        <allow role="Admin" />
    </ep>
</event_processors>
"""

config = f"""
HTTP_SERVER_PORT=48028
ALLOW_REMOTE_CONTROL=Yes
ALLOW_NO_CERTIFICATE=true
LOAD_ODBC_UDF=true
DB_LOCATOR.DEFAULT={current_path}/locator
ACCESS_CONTROL_FILE="{current_path}/acl.acl"
LICENSE_REPOSITORY_DIR="/license"
ONE_TICK_LICENSE_FILE="/license/license.dat"
TICK_SERVER_OTQ_CACHE_DIR={current_path}/dbs/otq_cache
OTQ_FILE_PATH="{current_path}/tests/core/otqs"
"""

locator = f"""
<VERSION_INFO VERSION="2"/>
<DATABASES>
    <db ID="COMMON" symbology="BZX" time_series_is_composite="YES">
        <locations>
            <location access_method="file" location="/opt/one_market_data/one_tick/examples/data/demo_level1"
                start_time="20001201000000" end_time="20301031050000" />
        </locations>
        <feed type="heartbeat_generator">
            <options format="native" />
        </feed>
    </db>

    <db ID="DEMO_L1" symbology="BZX" time_series_is_composite="YES">
        <locations>
            <location access_method="file" location="/opt/one_market_data/one_tick/examples/data/demo_level1"
                start_time="20001201000000" end_time="20301031050000" />
        </locations>
        <feed type="heartbeat_generator">
            <options format="native" />
        </feed>
    </db>

    {locator_dbs}
</DATABASES>

<TICK_SERVERS>
</TICK_SERVERS>

<CEP_TICK_SERVERS>
</CEP_TICK_SERVERS>

<INCLUDES>
</INCLUDES>
"""

with open(f"{current_path}/one_tick_config_webapi.txt", "w") as f:
    f.write(config)
with open(f"{current_path}/acl.acl", "w") as f:
    f.write(acl)
with open(f"{current_path}/locator", "w") as f:
    f.write(locator)
