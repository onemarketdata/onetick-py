import pytest
import onetick.py as otp
import numpy as np


@pytest.mark.parametrize('bins', [4, list(map(float, np.linspace(0, 10, num=5)))])
def test_cut(session, bins):
    data = otp.Ticks({"X": list(range(0, 11)), "Y": list(range(20, 31)), })
    data['bin'] = otp.cut(data['X'], bins=bins)
    df = otp.run(data)
    assert 'bin' in data.columns()
    assert df['bin'][0] == ', 2.5000000000]'
    assert df['bin'][6] == '(5.0000000000, 7.5000000000]'


@pytest.mark.parametrize('bins', [4, list(map(float, np.linspace(0, 10, num=5)))])
def test_cut_labeled(session, bins):
    data = otp.Ticks({"X": list(range(0, 11)), "Y": list(range(20, 31)), })
    labels = ["label" + str(i) for i in range(4)]
    data['bin'] = otp.cut(data['X'], bins=bins, labels=labels)
    df = otp.run(data)
    assert 'bin' in data.columns()
    assert df['bin'][0] == labels[0]
    assert df['bin'][9] == labels[3]


def test_qcut(session):
    data = otp.Ticks({"X": list(range(0, 11)), "Y": list(range(20, 31)), })
    data['bin'] = otp.qcut(data['X'], q=4)
    df = otp.run(data)
    assert 'bin' in data.columns()
    assert df['bin'][0] == ', 2.0000000000]'
    assert df['bin'][5] == '(2.0000000000, 5.0000000000]'


def test_qcut_labeled(session):
    data = otp.Ticks({"X": list(range(0, 11)), "Y": list(range(20, 31)), })
    labels = ["label" + str(i) for i in range(4)]
    data['bin'] = otp.qcut(data['X'], q=4, labels=labels)
    df = otp.run(data)
    assert 'bin' in data.columns()
    assert df['bin'][0] == labels[0]
    assert df['bin'][6] == labels[2]


def test_qcut_small(session):
    data = otp.Ticks({"X": [1]})
    data['bin'] = otp.qcut(data['X'], q=4)
    df = otp.run(data)
    assert 'bin' in data.columns()
    assert df['bin'][0] == ', 1.0000000000]'


def test_cut_small(session):
    data = otp.Ticks({"X": [1]})
    data['bin'] = otp.cut(data['X'], bins=4)
    df = otp.run(data)
    assert 'bin' in data.columns()
    assert df['bin'][0] == ', 1.0000000000]'


def test_qcut_list_exception(session):
    data = otp.Ticks({"X": [1]})
    with pytest.raises(NotImplementedError):
        data['bin'] = otp.qcut(data['X'], q=[0, 0.5, 1])


def test_cut_labeled_exception(session):
    data = otp.Ticks({"X": [9, 8, 5, 6, 7, 0]})
    with pytest.raises(ValueError, match='Number of labels is not equal to number of bins'):
        data['bin'] = otp.cut(data['X'], bins=3, labels=['o', 'n', 'e', 't', 'i', 'c', 'k'])
    with pytest.raises(ValueError, match='Number of labels is not equal to number of bins'):
        data['bin'] = otp.qcut(data['X'], q=3, labels=['o', 'n', 'e', 't', 'i', 'c', 'k'])


def test_cut_one_field_many_times(session):
    data = otp.Ticks({"X": [9, 8, 5, 6, 7, 0]})
    data['cut1'] = otp.cut(data['X'], bins=3)
    data['cut2'] = otp.cut(data['X'], bins=4)
    data['qcut1'] = otp.qcut(data['X'], q=3)
    data['qcut2'] = otp.qcut(data['X'], q=4)
    df = otp.run(data)
    assert set(df).issuperset({'cut1', 'cut2', 'qcut1', 'qcut2'})
