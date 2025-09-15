import onetick.py as otp


def test_construct_multi_branch_graph(session):
    source1 = otp.Tick(FIELD1=1)
    source2 = source1.copy()

    source1_jwq = otp.Tick(FIELD2='A')
    source2_jwq = otp.Tick(FIELD2='B')

    source1 = source1.join_with_query(source1_jwq)
    source1.node().node_name('OUT_1')
    source1.node()._ep.set_output_pin_name('OUT_1')
    source2 = source2.join_with_query(source2_jwq)
    source2.node().node_name('OUT_2')
    source2.node()._ep.set_output_pin_name('OUT_2')

    graph = otp.Source._construct_multi_branch_graph([source1, source2]).to_otq(add_passthrough=False)
    res = otp.run(graph)

    assert len(res) == 2
    assert 'OUT_1' in res.keys()
    assert 'OUT_2' in res.keys()
    assert len(res['OUT_1']) == 1
    assert res['OUT_1']['FIELD1'][0] == 1
    assert res['OUT_1']['FIELD2'][0] == 'A'
    assert len(res['OUT_2']) == 1
    assert res['OUT_2']['FIELD1'][0] == 1
    assert res['OUT_2']['FIELD2'][0] == 'B'
