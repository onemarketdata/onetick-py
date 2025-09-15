import os
import pytest

import onetick.py as otp
from onetick.py.otq import otq

if not (hasattr(otq, "ReadFromParquet") and hasattr(otq, "WriteToParquet")):
    pytest.skip("READ_FROM_PARQUET or WRITE_TO_PARQUET does not work on old OneTick versions", allow_module_level=True)


@pytest.mark.skipif(not otp.compatibility.is_write_parquet_directories_fixed(),
                    reason='not supported on older OneTick versions')
@pytest.mark.parametrize("write_opts,read_opts,expected_result", [
    ({}, {}, {"A": [1, 2, 3], "B": [4, 5, 6]}),
    ({"compression_type": "none"}, {"where": "A > 1 AND B < 6"}, {"A": [2], "B": [5]}),
    ({"propagate_input_ticks": True}, {"fields": ["A", "SYMBOL_NAME"]}, {"A": [1, 2, 3]}),
    ({}, {"discard_fields": ["A"]}, {"B": [4, 5, 6]}),
    ({}, {"fields": "A,SYMBOL_NAME"}, {"A": [1, 2, 3]}),
    ({}, {"discard_fields": "A"}, {"B": [4, 5, 6]}),
    ({}, {"symbol_name_field": "C"}, {"A": [1, 2], "B": [4, 5]})
])
def test_read_write(session, write_opts, read_opts, expected_result):
    tmpdir = otp.utils.TmpDir()
    file_path = os.path.join(tmpdir, "test_file")

    data = otp.Ticks(A=[1, 2, 3], B=[4, 5, 6], C=["TEST", "TEST", "AAA"], SYMBOL_NAME=["TEST"] * 3)
    data.write_parquet(file_path, inplace=True, **write_opts)
    write_result = otp.run(data)

    if write_opts.get("propagate_input_ticks", False):
        assert {"A": write_result["A"].to_list(), "B": write_result["B"].to_list()} == {
            "A": [1, 2, 3], "B": [4, 5, 6],
        }
    else:
        assert write_result["A"].empty and write_result["B"].empty

    data = otp.ReadParquet(file_path, **read_opts, symbol="TEST")
    result = otp.run(data)
    assert (
        ("A" in expected_result and result["A"].to_list() == expected_result["A"]) or
        ("A" not in expected_result and "A" not in result)
    )
    assert (
        ("B" in expected_result and result["B"].to_list() == expected_result["B"]) or
        ("B" not in expected_result and "B" not in result)
    )


def test_read_discard_fields_and_fields():
    with pytest.raises(ValueError, match="the same time"):
        otp.ReadParquet("/some/test/path", fields="A", discard_fields="B")

    with pytest.raises(ValueError, match="the same time"):
        otp.ReadParquet("/some/test/path", fields=["A"], discard_fields="B")

    with pytest.raises(ValueError, match="the same time"):
        otp.ReadParquet("/some/test/path", fields=["A"], discard_fields=["B"])

    with pytest.raises(ValueError, match="the same time"):
        otp.ReadParquet("/some/test/path", fields="A", discard_fields=["B"])
