import pytest

import onetick.py as otp
from onetick.py.compatibility import is_sha2_hashing_supported


@pytest.mark.parametrize(
    "hash_type,result", [
        ("sha_1", "2fd73fead4611f885b475edfd7069001be7ce6b1"),
        ("sha_224", "12d3f96511450121e6343b5ace065ec9de7b2a946b86f7dfab8ac51f"),
        ("sha_256", "539a374ff43dce2e894fd4061aa545e6f7f5972d40ee9a1676901fb92125ffee"),
        ("sha_384", "19e89a4af9c726e19fe696670b5eb26d7a2417e16a0ea6cfda03536d1b45b556b4b775639e4cc748ea0baf8fd58fb211"),
        (
            "sha_512",
            "277ccf6e8d034ce3cae96f35e41fcd1d1921792ee05b74f14045f20df5ca2d31c26d3d1a28cde2c67e85908262f0902c9d1f"
            "41eb33038bbabb2b670d7763bfec"
        ),
        ("lookup3", "3269863700000000"),
        ("metro_hash_64", "cb72458d8cdb7ab1"),
        ("city_hash_64", "dc45880ee23d745f"),
        ("murmur_hash_64", "0d997822c19cdcc6"),
        ("default", "f7edd10600000000"),
        ("sum_of_bytes", "aa04000000000000"),
    ]
)
def test_hash_code(session, hash_type, result):
    if is_sha2_hashing_supported():
        data = otp.Tick(A=otp.varstring('some_string'))
        data['HASH'] = otp.hash_code(data['A'], hash_type)
        df = otp.run(data)
        assert df["HASH"].to_list() == [result]


def test_hash_code_simple(session):
    if is_sha2_hashing_supported():
        data = otp.Tick(A=1)
        data['HASH'] = otp.hash_code('some_string', 'sha_256')
        df = otp.run(data)
        assert df["HASH"].to_list() == ["539a374ff43dce2e894fd4061aa545e6f7f5972d40ee9a1676901fb92125ffee"]


def test_incorrect_hash_type(session):
    data = otp.Tick(A='some_string')
    with pytest.raises(ValueError):
        data['HASH'] = otp.hash_code(data['A'], 'some_unknown_hash_type')
