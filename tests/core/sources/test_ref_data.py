import os
import pandas as pd
import pytest

import onetick.py as otp
import onetick.py.types as ott


class TestRefData:
    @pytest.fixture
    def ref_db_setup(self, f_session):
        db_name = 'DB'
        ref_db_name = f'REF_DATA_{db_name}'
        symbology = 'CORE'

        ref_db = otp.RefDB(
            ref_db_name,
            db_properties={
                'symbology': symbology,
            },
        )
        f_session.use(ref_db)

        db = otp.DB(
            db_name,
            db_properties={
                'ref_data_db': ref_db_name,
                'symbology': symbology,
            },
        )
        f_session.use(db)

        symbol_name_history = otp.Ticks(
            SYMBOL_NAME=['CORE_A'] * 2,
            SYMBOL_NAME_IN_HISTORY=['CORE_A', 'CORE_B'],
            SYMBOL_START_DATETIME=[otp.dt(2003, 12, 1)] * 2,
            SYMBOL_END_DATETIME=[otp.dt(2003, 12, 3)] * 2,
            START_DATETIME=[otp.dt(2003, 12, 1), otp.dt(2003, 12, 2)],
            END_DATETIME=[otp.dt(2003, 12, 2), otp.dt(2003, 12, 3)],
            offset=[0] * 2,
            db='LOCAL',
        )

        ref_db.put([
            otp.RefDB.SymbolNameHistory(symbol_name_history, symbology),
        ])

        yield f_session

    @pytest.mark.skipif(os.environ.get('OTP_WEBAPI', False), reason="WebAPI do not support reference database loader")
    @pytest.mark.skipif(os.name == "nt", reason="Works incorrectly on tests for Windows with older OneTick")
    def test_symbol_name_history(self, ref_db_setup):
        src = otp.RefData(
            'symbol_name_history',
            db='DB',
            symbol='CORE_B',
            start=otp.dt(2003, 12, 1),
            end=otp.dt(2003, 12, 3),
        )

        assert src.schema == {'END_DATETIME': ott.nsectime, 'SYMBOL_NAME': str}
        df = otp.run(src, symbol_date=otp.dt(2003, 12, 3)).to_dict(orient='list')
        del df['Time']
        assert df == {
            'END_DATETIME': [pd.Timestamp('2003-12-02 00:00:00'), pd.Timestamp('2003-12-03 00:00:00')],
            'SYMBOL_NAME': ['CORE_A', 'CORE_B'],
        }

    def test_exceptions(self):
        with pytest.raises(ValueError, match='was not set'):
            _ = otp.RefData()

        with pytest.raises(ValueError, match='Incorrect'):
            _ = otp.RefData('test')
