from onetick.py import RemoteTS, LoadBalancing, FaultTolerance

import pytest


@pytest.mark.parametrize("kwargs,result", [
    (dict(host='host1:4001'), 'host1:4001'),
    (dict(host='host1', port='4001'), 'host1:4001'),
    (dict(host='host1', port=4001), 'host1:4001'),
    (dict(host=FaultTolerance('host1:4001', 'host2:4002')), 'host1:4001,host2:4002'),
    (dict(host=FaultTolerance('host1:4001', 'host2:4002', 'host3:4003')), 'host1:4001,host2:4002,host3:4003'),
    (dict(host=LoadBalancing('host1:4001', 'host2:4002', 'host3:4003')), '(host1:4001,host2:4002,host3:4003)'),
    (dict(host=LoadBalancing('host1:4001', 'host2:4002')), '(host1:4001,host2:4002)'),
    (
        dict(host=FaultTolerance(LoadBalancing('host1:4001', 'host2:4002'), 'host3:4003')),
        '(host1:4001,host2:4002),host3:4003'
    ),
    (
        dict(host=FaultTolerance('host1:4001', LoadBalancing('host3:4003', 'host4:4004'))),
        'host1:4001,(host3:4003,host4:4004)'
    ),
    (
        dict(host=FaultTolerance(LoadBalancing('host1:4001', 'host2:4002'),
                                 LoadBalancing('host3:4003', 'host4:4004'))),
        '(host1:4001,host2:4002),(host3:4003,host4:4004)'
    ),
    (dict(host='wss://data.onetick.com:443/omdwebapi/websocket'), 'wss://data.onetick.com:443/omdwebapi/websocket'),
    (
        dict(protocol='wss', host='data.onetick.com', port=443, resource='omdwebapi/websocket'),
        'wss://data.onetick.com:443/omdwebapi/websocket'
    ),
    (dict(host='data.onetick.com', port=443), 'data.onetick.com:443'),
    (dict(protocol='http', host='data.onetick.com', port=443), 'http://data.onetick.com:443'),
])
def test_remote_ts_repr(kwargs, result):
    assert str(RemoteTS(**kwargs)) == result


def test_wrong_load_balancing():
    with pytest.raises(ValueError):
        LoadBalancing('host1:4001')


def test_wrong_fault_tolerance():
    with pytest.raises(ValueError):
        FaultTolerance('host1:4001')


def test_wrong_remote_ts():
    with pytest.raises(ValueError):
        RemoteTS(FaultTolerance('host1:4001', 'host2:4002'), 4004)


def test_with_protocol():
    with pytest.raises(ValueError):
        RemoteTS(host='wss://data.onetick.com:443/omdwebapi/websocket', port=444)
    with pytest.raises(ValueError):
        RemoteTS(host='http://data.onetick.com', port=443)
    with pytest.raises(ValueError):
        RemoteTS(host='wrong://data.onetick.com:443')
