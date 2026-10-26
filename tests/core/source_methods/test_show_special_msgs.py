import onetick.py as otp


def test_show_special_msgs(session):
    data = otp.Tick(X=1, bucket_interval=1)
    data = data.show_special_msgs()
    assert dict(data.schema) == {'MSG_TYPE': str, 'MSG_VAL': str}
    df = otp.run(data, running=True, start=otp.now(), end=otp.now() + otp.Second(1))
    assert list(df['MSG_TYPE']) == ['PREPARE_FOR_RUNNING_QUERY']
    assert list(df['MSG_VAL']) == ['']


def test_show_special_msgs_not_running(session):
    data = otp.Tick(X=1, bucket_interval=1)
    data = data.show_special_msgs()
    df = otp.run(data)
    assert df.empty
