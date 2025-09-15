import pytest
import onetick.py as otp


def test_error(session):
    data = otp.Tick(A=1)
    with pytest.raises(ValueError):
        data.add_fields({'A': 2})


def test_all(session):
    data = otp.Tick(A=1)
    data = data.add_fields({
        'D': otp.dt(2022, 2, 2),
        'X': 12345,
        'Y': data['A'],
        'Z': data['A'].astype(str) + 'abc',
    })
    assert data.schema['D'] is otp.nsectime
    assert data.schema['X'] is int
    assert data.schema['Y'] is int
    assert data.schema['Z'] is str
    df = otp.run(data)
    assert df['D'][0] == otp.dt(2022, 2, 2)
    assert df['X'][0] == 12345
    assert df['Y'][0] == 1
    assert df['Z'][0] == '1abc'


@pytest.mark.skipif(not otp.compatibility.is_existing_fields_handling_supported(),
                    reason='not supported on old OneTick builds')
def test_override(session):
    data = otp.Tick(A=1)
    data = data.add_fields({'A': 'a', 'B': 12345}, override=True)
    assert data.schema['A'] is str
    assert data.schema['B'] is int
    df = otp.run(data)
    assert df['A'][0] == 'a'
    assert df['B'][0] == 12345
