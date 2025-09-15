import numpy as np
import pandas as pd

import onetick.py as otp


def test_apply_int2str_add_1(session):
    d = {"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = data.x.apply(str)
    df["t"] = df.x.apply(str)

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["1", "2"]
    assert df.t[0] == "1" and df.t[1] == "2"

    data.t += "abc"
    df.t += "abc"

    assert list(otp.run(data).t) == ["1abc", "2abc"]
    assert df.t[0] == "1abc" and df.t[1] == "2abc"


def test_apply_int2str_add_2(session):
    d = {"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = "xxx " + data.x.apply(str) + " abc"
    df["t"] = "xxx " + df.x.apply(str) + " abc"

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["xxx 1 abc", "xxx 2 abc"]
    assert df.t[0] == "xxx 1 abc" and df.t[1] == "xxx 2 abc"


def test_apply_float2str_add_1(session):
    d = {"x": [1, 2], "y": [0.73491245, -1.345052], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = data.y.round(1).apply(str)
    df["t"] = df.y.round(1).apply(str)

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["0.7", "-1.3"]
    assert df.t[0] == "0.7" and df.t[1] == "-1.3"

    data.t += "abc"
    df.t += "abc"

    assert list(otp.run(data).t) == ["0.7abc", "-1.3abc"]
    assert df.t[0] == "0.7abc" and df.t[1] == "-1.3abc"


def test_apply_float2str_add_2(session):
    d = {"x": [1, 2], "y": [0.73491245, -1.345052], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = data.y.round(2).apply(str)
    df["t"] = df.y.round(2).apply(str)

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["0.73", "-1.35"]
    assert df.t[0] == "0.73" and df.t[1] == "-1.35"

    data.t = "[" + data.t + "]"
    df.t = "[" + df.t + "]"

    assert list(otp.run(data).t) == ["[0.73]", "[-1.35]"]
    assert df.t[0] == "[0.73]" and df.t[1] == "[-1.35]"


def test_apply_float2str_add_3(session):
    d = {"x": [1, 2], "y": [0.73491245, -1.345052], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = data.y.round(5).apply(str)
    df["t"] = df.y.round(5).apply(str)

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["0.73491", "-1.34505"]
    assert df.t[0] == "0.73491" and df.t[1] == "-1.34505"

    data.t = "[" + data.t + "]"
    df.t = "[" + df.t + "]"

    assert list(otp.run(data).t) == ["[0.73491]", "[-1.34505]"]
    assert df.t[0] == "[0.73491]" and df.t[1] == "[-1.34505]"


def test_apply_float2str_add_4(session):
    d = {"x": [1, 2], "y": [0.73491245, -1.345052], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = data.y.round(0).apply(str)
    df["t"] = df.y.round(0).apply(str)

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["1.0", "-1.0"]
    assert df.t[0] == "1.0" and df.t[1] == "-1.0"

    data.t = "[" + data.t + "]"
    df.t = "[" + df.t + "]"

    assert list(otp.run(data).t) == ["[1.0]", "[-1.0]"]
    assert df.t[0] == "[1.0]" and df.t[1] == "[-1.0]"


def test_apply_float2str_add_5(session):
    d = {"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = data.y.apply(str)
    df["t"] = df.y.apply(str)

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["0.7", "-1.3"]
    assert df.t[0] == "0.7" and df.t[1] == "-1.3"

    data.t = "[" + data.t + "]"
    df.t = "[" + df.t + "]"

    assert list(otp.run(data).t) == ["[0.7]", "[-1.3]"]
    assert df.t[0] == "[0.7]" and df.t[1] == "[-1.3]"


def test_apply_float2str_add_6(session):
    d = {"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = data.y.apply(str)
    df["t"] = df.y.apply(str)

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["0.7", "-1.3"]
    assert df.t[0] == "0.7" and df.t[1] == "-1.3"

    data.t = "[" + data.t + "]"
    df.t = "[" + df.t + "]"

    assert list(otp.run(data).t) == ["[0.7]", "[-1.3]"]
    assert df.t[0] == "[0.7]" and df.t[1] == "[-1.3]"


def test_apply_str2float_add_1(session):
    d = {"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = data.z.apply(float)
    df["t"] = df.z.apply(float)

    res = otp.run(data)
    assert hasattr(data, "t") and data.t.dtype is float
    assert isinstance(res.t[0], np.float64)
    assert list(res.t) == [3.5, -0.1]
    assert df.t[0] == 3.5 and df.t[1] == -0.1

    data.t += 0.1
    df.t += 0.1

    assert list(otp.run(data).t) == [3.6, 0.0]
    assert df.t[0] == 3.6 and df.t[1] == 0.0


def test_apply_str2float_add_2(session):
    d = {"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = -data.z.apply(float) + 1
    df["t"] = -df.z.apply(float) + 1

    res = otp.run(data)
    assert hasattr(data, "t") and data.t.dtype is float
    assert isinstance(res.t[0], np.float64)
    assert list(res.t) == [-2.5, 1.1]
    assert df.t[0] == -2.5 and df.t[1] == 1.1


def test_apply_str2int_add_1(session):
    d = {"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = data.z.apply(int)
    df["t"] = df.z.apply(float).apply(int)

    res = otp.run(data)
    assert hasattr(data, "t") and data.t.dtype is int
    assert isinstance(res.t[0], np.integer)
    assert list(res.t) == [3, 0]
    assert df.t[0] == 3 and df.t[1] == 0

    data.t += 0.1
    df.t += 0.1

    assert list(otp.run(data).t) == [3.1, 0.1]
    assert df.t[0] == 3.1 and df.t[1] == 0.1


def test_apply_str2int_add_2(session):
    d = {"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = -data.z.apply(int) + 4
    # pandas doesn't have direct transformation from str -> int
    df["t"] = -df.z.apply(float).apply(int) + 4

    res = otp.run(data)
    assert hasattr(data, "t") and data.t.dtype is int
    assert isinstance(res.t[0], np.integer)
    assert list(res.t) == [1, 4]
    assert df.t[0] == 1 and df.t[1] == 4


def test_apply_str2int_large_number(session):
    src = otp.Ticks(X=['1688964028797322000'])
    src['X'] = src['X'].apply(int)
    assert src.schema['X'] == int
    df = otp.run(src)
    assert df['X'].dtype == np.dtype('int64')
    assert df['X'][0] == 1688964028797322000


def test_astype_int2str_add_1(session):
    d = {"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data.t = data.x.astype(str)
    df["t"] = df.x.astype(str)

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["1", "2"]
    assert df.t[0] == "1" and df.t[1] == "2"

    data.t += "abc"
    df.t += "abc"

    assert list(otp.run(data).t) == ["1abc", "2abc"]
    assert df.t[0] == "1abc" and df.t[1] == "2abc"


def test_astype_int2str_add_2(session):
    data = otp.Ticks({"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"], "offset": [1, 2]})

    data.t = "abc " + data.x.astype(str) + " xxx"

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["abc 1 xxx", "abc 2 xxx"]


def test_astype_float2str_add_1(session):
    data = otp.Ticks({"x": [1, 2], "y": [0.73491245, -1.345052], "z": ["3.5", "-0.1"], "offset": [1, 2]})

    data.t = data.y.round(2).astype(str)

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["0.73", "-1.35"]

    data.t += "abc"

    assert list(otp.run(data).t) == ["0.73abc", "-1.35abc"]


def test_astype_float2str_add_2(session):
    data = otp.Ticks({"x": [1, 2], "y": [0.73491245, -1.345052], "z": ["3.5", "-0.1"], "offset": [1, 2]})

    data.t = "xxx " + data.y.round(2).astype(str) + " abc"

    assert hasattr(data, "t") and data.t.dtype is str
    assert list(otp.run(data).t) == ["xxx 0.73 abc", "xxx -1.35 abc"]


def test_astype_str2float_add_1(session):
    data = otp.Ticks({"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"], "offset": [1, 2]})

    data.t = data.z.astype(float)

    res = otp.run(data)
    assert hasattr(data, "t") and data.t.dtype is float
    assert isinstance(res.t[0], np.float64)
    assert list(res.t) == [3.5, -0.1]

    data.t += 0.1

    assert list(otp.run(data).t) == [3.6, 0.0]


def test_astype_str2float_add_2(session):
    data = otp.Ticks({"x": [1, 2], "y": [0.7, -1.3], "z": ["3.5", "-0.1"], "offset": [1, 2]})

    data.t = -data.z.astype(float) + 0.1

    res = otp.run(data)
    assert hasattr(data, "t") and data.t.dtype is float
    assert isinstance(res.t[0], np.float64)
    assert list(res.t) == [-3.4, 0.2]


def test_astype_str2int_add_1(session):
    data = otp.Ticks({"x": [1, 2], "y": [0.7, -1.3], "z": ["3.4", "-0.1"], "offset": [1, 2]})

    data.t = data.z.astype(int)

    res = otp.run(data)
    assert hasattr(data, "t") and data.t.dtype is int
    assert isinstance(res.t[0], np.integer)
    assert list(res.t) == [3, 0]

    data.t += 0.1

    assert list(otp.run(data).t) == [3.1, 0.1]


def test_astype_str2int_add_2(session):
    data = otp.Ticks({"x": [1, 2], "y": [0.7, -1.3], "z": ["3.4", "-0.1"], "offset": [1, 2]})

    data.t = -data.z.astype(int) + 5

    res = otp.run(data)
    assert hasattr(data, "t") and data.t.dtype is int
    assert isinstance(res.t[0], np.integer)
    assert list(res.t) == [2, 5]


def test_float2int(session):
    data = otp.Ticks({"x": [1.1, 1.7, -1.2, -1.8]})
    df = pd.DataFrame({"x": [1.1, 1.7, -1.2, -1.8]})

    df["t"] = -df.x.apply(int) + 5
    data["t"] = -data.x.apply(int) + 5

    res = otp.run(data)
    assert isinstance(res.t[0], np.integer)
    assert df.t[0] == res.t[0]
    assert df.t[1] == res.t[1]
    assert df.t[2] == res.t[2]
    assert df.t[3] == res.t[3]


def test_float2int_samecolumn(session):
    data = otp.Ticks({"x": [1.1, 1.7, -1.2, -1.8]})
    data["x"] = data.x.apply(int)
    assert data.schema["x"] == int


def test_int2float(session):
    d = {"x": [1, 2, 3, 4], "y": [0.7, -1.3, 0, 0], "z": ["3.1", "3.8", "-0.1", "-0.9"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data["t"] = -data.x.astype(float) + 3
    df["t"] = -df.x.astype(float) + 3

    res = otp.run(data)
    assert isinstance(res.t[0], np.float64)
    assert list(res.t) == list(df.t)


def test_int2nsectime(session):
    data = otp.Tick(X=1)
    data['TI'] = data['Time'].apply(int)
    data['T'] = data['TI'].apply(otp.nsectime)
    res = otp.run(data)
    assert all(res['Time'] == res['T'])


def test_str2float(session):
    d = {"x": [1, 2, 3, 4], "y": [0.7, -1.3, 0, 0], "z": ["3.1", "3.8", "-0.1", "-0.9"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data["t"] = -data.z.astype(float) + 0.5
    df["t"] = -df.z.astype(float) + 0.5

    res = otp.run(data)
    assert isinstance(res.t[0], np.float64)
    assert list(res.t) == list(df.t)


def test_float2str(session):
    d = {"x": [1, 2, 3, 4], "y": [0.7, -1.3, 0, 9.0], "z": ["3.1", "3.8", "-0.1", "-0.9"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data["t"] = "xxx " + data.y.astype(str) + " abc"
    df["t"] = "xxx " + df.y.astype(str) + " abc"

    res = otp.run(data)
    assert isinstance(res.t[0], str)
    assert list(res.t) == list(df.t)


def test_str2int(session):
    d = {"x": [1, 2, 3, 4], "y": [0.7, -1.3, 0, 0], "z": ["3", "1", "-1", "-9"]}

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data["t"] = -data.z.astype(int) + 4
    df["t"] = -df.z.astype(int) + 4

    res = otp.run(data)
    assert isinstance(res.t[0], np.integer)
    assert list(res.t) == list(df.t)


def test_datetime2str_1(session):
    d = {"x": [1, 2, 3, 4]}

    data = otp.Ticks(d)
    df = pd.DataFrame({"Time": otp.run(data).Time.tolist()})

    data["t"] = data.Time.apply(str)
    df["t"] = df.Time.apply(str)

    assert data.t.dtype is str
    res = otp.run(data)
    assert res.t[0] == df.t[0] + ".000000000"
    # somewhy pandas dont remove trailing zeros
    assert res.t[1] == df.t[1] + "000"
    assert res.t[2] == df.t[2] + "000"
    assert res.t[3] == df.t[3] + "000"


def test_datetime2str_2(session):
    d = {"x": [1, 2, 3, 4]}

    data = otp.Ticks(d)
    df = pd.DataFrame({"Time": otp.run(data).Time.tolist()})

    data["t"] = data.Time
    df["t"] = df.Time

    data.t = data.t.apply(str)
    df.t = df.t.apply(str)

    assert data.t.dtype is str
    res = otp.run(data)
    assert res.t[0] == df.t[0] + ".000000000"
    # somwhy pandas dont remove trailing zeros
    assert res.t[1] == df.t[1] + "000"
    assert res.t[2] == df.t[2] + "000"
    assert res.t[3] == df.t[3] + "000"


def test_datetime2str_3(session):
    d = {"x": [1, 2, 3, 4]}

    data = otp.Ticks(d)
    df = pd.DataFrame({"Time": otp.run(data).Time.tolist()})

    data["t"] = "abc " + data.Time.apply(str) + " xxx"
    df["t"] = "abc " + df.Time.apply(str) + "000 xxx"

    assert data.t.dtype is str
    res = otp.run(data)
    # somwhy pandas dont remove trailing zeros
    assert res.t[1] == df.t[1]
    assert res.t[2] == df.t[2]
    assert res.t[3] == df.t[3]


def test_str2datetime_1(session):
    d = {"x": [1, 2, 3, 4]}

    data = otp.Ticks(d)
    df = pd.DataFrame({"Time": otp.run(data).Time.tolist()})

    data["t"] = data.Time.apply(str)
    df["t"] = df.Time.apply(str)

    data["z"] = data.t.apply(otp.msectime)
    df["z"] = df.t.apply(pd.Timestamp)

    assert data.t.dtype is str
    assert data.z.dtype is otp.msectime

    res = otp.run(data)
    assert res.z[0] == df.z[0]
    assert res.z[1] == df.z[1]
    assert res.z[2] == df.z[2]
    assert res.z[3] == df.z[3]


def test_str2datetime_2(session):
    d = {
        "some_time": [
            "2005-12-01 00:00:01.000",
            "2005-12-02 13:00:01.090",
            "2005-11-02 17:22:01.001",
            "2018-01-22 01:45:34.123",
        ]
    }

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data["t"] = data.some_time.apply(otp.msectime)
    df["t"] = df.some_time.apply(pd.Timestamp)

    assert data.t.dtype is otp.msectime

    res = otp.run(data)
    assert res.t[0] == df.t[0]
    assert res.t[1] == df.t[1]
    assert res.t[2] == df.t[2]
    assert res.t[3] == df.t[3]


def test_str2datetime_3(session):
    d = {
        "some_time": [
            "2005/12/01 00:00:01.000",
            "2005/12/02 13:00:01.090",
            "2005/11/02 17:22:01.001",
            "2018/01/22 01:45:34.123",
        ]
    }

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data["t"] = data.some_time.apply(otp.msectime)
    df["t"] = df.some_time.apply(pd.Timestamp)

    assert data.t.dtype is otp.msectime

    res = otp.run(data)
    assert res.t[0] == df.t[0]
    assert res.t[1] == df.t[1]
    assert res.t[2] == df.t[2]
    assert res.t[3] == df.t[3]


def test_str2datetime_4(session):
    d = {
        "some_time": [
            "2005/12/01 00:00:01.000",
            "2005/12/02 13:00:01.090",
            "2005/11/02 17:22:01.001",
            "2018/01/22 01:45:34.123",
        ]
    }

    data = otp.Ticks(d)
    df = pd.DataFrame(d)

    data["t"] = 100 + data.some_time.apply(otp.msectime)
    df["t"] = df.some_time.apply(pd.Timestamp) + otp.Milli(100)

    assert data.t.dtype is otp.msectime

    res = otp.run(data)
    assert res.t[0] == df.t[0]
    assert res.t[1] == df.t[1]
    assert res.t[2] == df.t[2]
    assert res.t[3] == df.t[3]


def test_str2nsecime(session):
    d = {"some_time": ["2005/12/01 01:45:34.123456789"]}

    data = otp.Ticks(d)

    data.msectime_t = data.some_time.apply(otp.msectime)
    data.nsectime_t = data.some_time.apply(otp.nsectime)

    assert data.msectime_t.dtype is otp.msectime
    assert data.nsectime_t.dtype is otp.nsectime

    df = otp.run(data)

    s = pd.to_datetime(df["some_time"], format="%Y/%m/%d %H:%M:%S.%f")

    assert df.nsectime_t[0].value == s[0].value
    assert df.nsectime_t[0].value - df.msectime_t[0].value == 456789


def test_str2str(session):
    data = otp.Ticks(dict(A=["A" * 64, "B" * 64]))
    data["A"] = data["A"].apply(str)
    df = otp.run(data)
    assert len(df) == 2
    assert all(df["A"].str.len() == 64)


def test_nsectime2int(session):
    data = otp.Ticks(X=[1, 2, 3])
    data['T'] = data['Time'] + otp.Nano(data['X'])
    data['TI'] = data['T'].apply(int) + 4

    df = otp.run(data, timezone='GMT')

    for inx in range(len(df)):
        assert df['TI'][inx] == df['T'][inx].value + 4


def test_varstring(session):
    data = otp.Ticks(X=["Mr. Smith: +1348 +4781", "Ms. Smith: +8971"])
    data = data.table(X=otp.varstring)
    data["TEL"] = data["X"].str.extract(r"\+\d{4}")
    assert data["TEL"].dtype is otp.varstring


def test_apply_from_otp_string(session):
    t = otp.Tick(A=1)

    t['SI'] = otp.string[8]('12345')
    t['I'] = t['SI'].apply(int)

    t['SF'] = otp.string[16]('12345.12345')
    t['F'] = t['SF'].apply(float)

    t['SD'] = otp.string[32]('2023/04/05 01:02:03.123456789')
    t['MS'] = t['SD'].apply(otp.msectime)
    t['NS'] = t['SD'].apply(otp.nsectime)

    df = otp.run(t)

    assert t.schema['I'] is int
    assert df.dtypes['I'] == np.int64
    assert df['I'][0] == 12345

    assert t.schema['F'] is float
    assert df.dtypes['F'] == np.float64
    assert df['F'][0] == 12345.12345

    assert t.schema['MS'] is otp.msectime
    assert df.dtypes['MS'].type == np.datetime64
    assert df['MS'][0] == otp.dt(2023, 4, 5, 1, 2, 3, 123000)

    assert t.schema['NS'] is otp.nsectime
    assert df.dtypes['NS'].type == np.datetime64
    assert df['NS'][0] == otp.dt(2023, 4, 5, 1, 2, 3, 123456, 789)


def test_apply_to_otp_string(session):
    t = otp.Tick(A=1)

    t['I'] = 12345
    t['SI'] = t['I'].apply(otp.string[8])

    t['F'] = 12345.12345
    t['SF'] = t['F'].apply(otp.string[16])

    t['NS'] = otp.dt('2023-04-05 01:02:03.123456789')
    t['MS'] = t['NS']
    t = t.table(MS=otp.msectime, strict=False)
    t['SNS'] = t['NS'].apply(otp.string[32])
    t['SMS'] = t['MS'].apply(otp.string[32])

    df = otp.run(t)

    assert t.schema['SI'] is otp.string[8]
    assert pd.api.types.is_string_dtype(df['SI'])
    assert df['SI'][0] == '12345'

    assert t.schema['SF'] is otp.string[16]
    assert pd.api.types.is_string_dtype(df['SF'])
    assert df['SF'][0] == '12345.12345'

    assert t.schema['MS'] is otp.msectime
    assert pd.api.types.is_string_dtype(df['SMS'])
    assert df['SMS'][0] == '2023-04-05 01:02:03.123'

    assert t.schema['NS'] is otp.nsectime
    assert pd.api.types.is_string_dtype(df['SNS'])
    assert df['SNS'][0] == '2023-04-05 01:02:03.123456789'


def test_apply_from_to_otp_string(session):
    t = otp.Tick(A=1)

    t['S8'] = otp.string[8]('12345678')
    t['S16'] = t['S8'].apply(otp.string[16])
    t['S4'] = t['S8'].apply(otp.string[4])

    df = otp.run(t)

    assert t.schema['S16'] is otp.string[16]
    assert pd.api.types.is_string_dtype(df['S16'])
    assert df['S16'][0] == '12345678'

    assert t.schema['S4'] is otp.string[4]
    assert pd.api.types.is_string_dtype(df['S4'])
    assert df['S4'][0] == '1234'


def test_to_datetime(session):
    t = otp.Tick(INT=1)
    t['MS'] = otp.msectime(1)
    t['NS'] = otp.dt(1970, 1, 1, 0, 0, 0, 0, nanosecond=1)
    t['INT_MS'] = t['INT'].apply(otp.msectime)
    t['INT_NS'] = t['INT'].apply(otp.nsectime)
    t['MS_INT'] = t['MS'].apply(int)
    t['MS_NS'] = t['MS'].apply(otp.nsectime)
    t['NS_INT'] = t['NS'].apply(int)
    t['NS_MS'] = t['NS'].apply(otp.msectime)
    df = otp.run(t, timezone='GMT')
    ms = pd.Timestamp(1970, 1, 1, 0, 0, 0, 1000)
    ns = otp.datetime(1970, 1, 1, 0, 0, 0, 0, 1).ts
    assert df['MS'][0] == ms
    assert df['NS'][0] == ns
    assert df['INT_MS'][0] == ms
    assert df['INT_NS'][0] == ns
    assert df['MS_INT'][0] == 1
    assert df['MS_NS'][0] == ms
    assert df['NS_INT'][0] == 1
    assert df['NS_MS'][0] == pd.Timestamp(1970, 1, 1, 0, 0, 0)


def test_bool_to_int(session):
    t = otp.Ticks(A=[True, False])
    t['RES'] = t['A'].astype(int)

    df = otp.run(t)
    assert df['RES'].array == [1, 0]


def test_bool_to_float(session):
    t = otp.Ticks(A=[True, False])
    t['RES'] = t['A'].astype(float)

    df = otp.run(t)
    assert df['RES'].array == [1.0, 0.0]


def test_operation_bool_conversions(session):
    t = otp.Ticks(A=[1, 2, 3])
    t['eq'] = (t['A'] == 2).astype(int)
    t['lt'] = (t['A'] < 2).astype(int)
    t['le'] = (t['A'] <= 2).astype(int)
    t['gt'] = (t['A'] > 2).astype(int)
    t['ge'] = (t['A'] >= 2).astype(int)

    df = otp.run(t)
    assert df['eq'].array == [0, 1, 0]
    assert df['lt'].array == [1, 0, 0]
    assert df['le'].array == [1, 1, 0]
    assert df['gt'].array == [0, 0, 1]
    assert df['ge'].array == [0, 1, 1]


def test_operation_bool_conversions_complex(session):
    t = otp.Ticks(A=[1, 2, 3])
    t['t1'] = (t['A'] == 2) == (t['A'] == 1)
    t['t2'] = ((t['A'] == 2) == (t['A'] == 2)) == (t['A'] == 2)  # NOSONAR

    df = otp.run(t)
    assert df['t1'].array == [0.0, 0.0, 1.0]
    assert df['t2'].array == [0.0, 1.0, 0.0]


def test_convert_to_string(session):
    t = otp.Tick(
        A=1,
        B=1.1,
        C='c',
        D=otp.string[23]('d'),
        E=otp.varstring('e'),
        F=otp.nsectime(0),
        G=otp.msectime(1),
        H=otp.int(3),
        I=otp.short(4),
        J=otp.byte(5),
        K=otp.uint(6),
        L=otp.ulong(7),
        M=otp.decimal(8.8),
        N=otp.long(9)
    )

    t['SA'] = t['A'].astype(str)
    t['SB'] = t['B'].astype(str)
    t['SC'] = t['C'].astype(str)
    t['SD'] = t['D'].astype(str)
    t['SE'] = t['E'].astype(str)
    t['SF'] = t['F'].astype(str)
    t['SG'] = t['G'].astype(str)
    t['SH'] = t['H'].astype(str)
    t['SI'] = t['I'].astype(str)
    t['SJ'] = t['J'].astype(str)
    t['SK'] = t['K'].astype(str)
    t['SL'] = t['L'].astype(str)
    t['SM'] = t['M'].astype(str)
    t['SN'] = t['N'].astype(str)

    assert t.schema == dict(
        A=int,
        B=float,
        C=str,
        D=otp.string[23],
        E=otp.varstring,
        F=otp.nsectime,
        G=otp.msectime,
        H=otp.int,
        I=otp.short,
        J=otp.byte,
        K=otp.uint,
        L=otp.ulong,
        M=otp.decimal,
        N=otp.long,
        SA=str,
        SB=str,
        SC=str,
        SD=str,
        SE=str,
        SF=str,
        SG=str,
        SH=str,
        SI=str,
        SJ=str,
        SK=str,
        SL=str,
        SM=str,
        SN=str,
    )

    df = otp.run(t)
    assert dict(df.iloc[0]) == {
        'Time': pd.Timestamp('2003-12-01 00:00:00'),
        'A': 1,
        'B': 1.1,
        'C': 'c',
        'D': 'd',
        'E': 'e',
        'F': pd.Timestamp('1969-12-31 19:00:00'),
        'G': pd.Timestamp('1969-12-31 19:00:00.001000'),
        'H': 3,
        'I': 4,
        'J': 5,
        'K': 6,
        'L': 7,
        'M': 8.8,
        'N': 9,
        'SA': '1',
        'SB': '1.1',
        'SC': 'c',
        'SD': 'd',
        'SE': 'e',
        'SF': '1969-12-31 19:00:00.000000000',
        'SG': '1969-12-31 19:00:00.001',
        'SH': '3',
        'SI': '4',
        'SJ': '5',
        'SK': '6',
        'SL': '7',
        'SM': '8.80000000',
        'SN': '9',
    }


def test_astype_int_combinations(session):
    for type_ in (otp.short, otp.int, otp.byte, otp.long, int):
        src = otp.Ticks(A=[1, 2, 3],
                        B=[1.1, 2.2, 3.3],
                        C=['1', '2', '3'],
                        D=[otp.dt(2022, 1, i + 1) for i in range(3)])
        src['AA'] = src['A'].astype(type_)
        src['BB'] = src['B'].astype(type_)
        src['CC'] = src['C'].astype(type_)
        src['DD'] = src['D'].astype(type_)
        assert src.schema['AA'] is type_
        assert src.schema['BB'] is type_
        assert src.schema['CC'] is type_
        assert src.schema['DD'] is type_
