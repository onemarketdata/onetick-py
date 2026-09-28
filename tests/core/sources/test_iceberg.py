import datetime
from pathlib import Path

import pytest

import onetick.py as otp

if not otp.compatibility._is_read_from_iceberg_supported():
    pytest.skip("READ_FROM_ICEBERG does not work on old OneTick versions", allow_module_level=True)


CATALOG_CONFIG = "/path/to/catalog.cfg"
TABLE_IDENTIFIER = "reporting.sales_data"


def get_iceberg_ep(data):
    """
    Get the string representation of the READ_FROM_ICEBERG EP from the generated query.
    Quotes around string values are removed to simplify the checks.
    """
    text = Path(data.to_otq().split("::")[0]).read_text()
    lines = [line for line in text.splitlines() if "READ_FROM_ICEBERG" in line]
    assert len(lines) == 1
    return lines[0].replace('"', '')


def test_read_from_iceberg_exceptions():
    with pytest.raises(ValueError, match="Missing required parameter `catalog_config`"):
        otp.ReadFromIceberg(table_identifier=TABLE_IDENTIFIER)

    with pytest.raises(ValueError, match="Missing required parameter `table_identifier`"):
        otp.ReadFromIceberg(catalog_config=CATALOG_CONFIG)

    with pytest.raises(ValueError, match="Incorrect value for parameter `time_assignment`"):
        otp.ReadFromIceberg(CATALOG_CONFIG, TABLE_IDENTIFIER, time_assignment="trade_time")

    with pytest.raises(ValueError, match="Parameter `where` must be of type str"):
        otp.ReadFromIceberg(CATALOG_CONFIG, TABLE_IDENTIFIER, where=12345)

    for as_of_time in ("20230101120000", True, 1.5):
        with pytest.raises(ValueError, match="Parameter `as_of_time` must be of type"):
            otp.ReadFromIceberg(CATALOG_CONFIG, TABLE_IDENTIFIER, as_of_time=as_of_time)


def test_required_parameters(session):
    data = otp.ReadFromIceberg(CATALOG_CONFIG, TABLE_IDENTIFIER)
    ep = get_iceberg_ep(data)
    assert f'CATALOG_CONFIG={CATALOG_CONFIG}' in ep
    assert f'TABLE_IDENTIFIER={TABLE_IDENTIFIER}' in ep


@pytest.mark.parametrize("fields,expected", [
    (["A", "B"], "FIELDS=A,B"),
    (("A", "B"), "FIELDS=A,B"),
    ("A,B", "FIELDS=A,B"),
    ("A", "FIELDS=A"),
])
def test_fields(session, fields, expected):
    data = otp.ReadFromIceberg(CATALOG_CONFIG, TABLE_IDENTIFIER, fields=fields)
    assert expected in get_iceberg_ep(data)


@pytest.mark.parametrize("as_of_time", [
    otp.datetime(2023, 1, 1, 12),
    datetime.datetime(2023, 1, 1, 12),
])
def test_as_of_time(session, as_of_time):
    expected = otp.datetime(2023, 1, 1, 12).ts.tz_localize(otp.config.tz).value // 1_000_000
    data = otp.ReadFromIceberg(CATALOG_CONFIG, TABLE_IDENTIFIER, fields="A", as_of_time=as_of_time)
    assert f"AS_OF_TIME={expected}" in get_iceberg_ep(data)


def test_as_of_time_int(session):
    data = otp.ReadFromIceberg(CATALOG_CONFIG, TABLE_IDENTIFIER, fields="A", as_of_time=1672574400000)
    assert "AS_OF_TIME=1672574400000" in get_iceberg_ep(data)


@pytest.mark.skipif(not otp.compatibility._is_read_from_iceberg_drop_fields_supported(),
                    reason="not supported on older OneTick versions")
def test_drop_fields(session):
    data = otp.ReadFromIceberg(CATALOG_CONFIG, TABLE_IDENTIFIER, fields="A", drop_fields=True)
    assert "DROP_FIELDS=True" in get_iceberg_ep(data)
