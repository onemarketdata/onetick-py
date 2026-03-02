from io import StringIO
from textwrap import dedent

import pandas as pd
import pytest

import onetick.py as otp
from onetick.py.otq import otq


class TestReadFromDataFrame:
    def get_test_data(
        self, timestamp_column='TIME', symbol_name_field=None, symbol_name=None, add_special_values=False,
    ):
        header = f"{timestamp_column},ID,SIDE,PRICE,FLOAT"
        data = [
            "2024-01-01 12:00:00.001,1,BUY,50.05,50.05",
            "2024-01-01 12:00:02.000,1,SELL,50.05,50.05",
            "2024-01-01 12:00:02.500,2,BUY,49.95,49.95",
            "2024-01-01 12:00:03.100,2,SELL,49.98,49.98",
            "2024-01-01 12:00:03.250,3,BUY,50.02,50.02",
        ]

        if add_special_values:
            data.append("2024-01-01 12:10:10.100,3,BUY,49.98,nan")

        if symbol_name_field:
            header += f',{symbol_name_field}'
            data = [line + f',{symbol_name}' for line in data]

        return pd.read_csv(StringIO("\n".join([header, *data])))

    def make_queries(self, dataframe, schema=None, kwargs=None, drop_compare_for=None):
        if kwargs is None:
            kwargs = {}

        if schema is None:
            schema = {}

        native_result = None
        if hasattr(otq, 'ReadFromDataFrame'):
            src = otp.ReadFromDataFrame(dataframe.copy(), **kwargs)
            assert src.schema == schema
            native_result = otp.run(src, date=otp.date(2024, 1, 1))

        src = otp.ReadFromDataFrame(dataframe.copy(), force_compatibility_mode=True, **kwargs)
        assert src.schema == schema
        compat_result = otp.run(src, date=otp.date(2024, 1, 1))

        if native_result is not None:
            native_dict = native_result.to_dict(orient='list')
            compat_dict = compat_result.to_dict(orient='list')
            if drop_compare_for:
                for col in drop_compare_for:
                    del native_dict[col]
                    del compat_dict[col]

            assert native_dict == compat_dict

        return native_result, compat_result

    @pytest.mark.parametrize('timestamp_column', [
        'TIME', 'TiMe', 'Time', 'TIMESTAMP', 'Timestamp',
    ])
    def test_timestamp_column_auto(self, session, timestamp_column):
        dataframe = self.get_test_data(timestamp_column=timestamp_column, add_special_values=False)

        src_schema = {
            'ID': int,
            'SIDE': str,
            'PRICE': float,
            'FLOAT': float,
            'SYMBOL_NAME': str,
        }

        _, compat = self.make_queries(dataframe, schema=src_schema)

        if timestamp_column != 'TIMESTAMP':
            # Check explicit timestamp_column set
            _, compat_with_ts = self.make_queries(dataframe, schema=src_schema, kwargs={
                'symbol': 'AAPL', 'timestamp_column': timestamp_column,
            })
            assert compat.to_dict(orient='list') == compat_with_ts.to_dict(orient='list')

        result_schema = {'Time', 'ID', 'SIDE', 'PRICE', 'FLOAT', 'SYMBOL_NAME'}

        assert len(compat) == 5
        assert set(compat.columns) == result_schema

        assert compat['Time'].to_list() == [
            pd.Timestamp('2024-01-01 12:00:00.001'), pd.Timestamp('2024-01-01 12:00:02.000'),
            pd.Timestamp('2024-01-01 12:00:02.500'), pd.Timestamp('2024-01-01 12:00:03.100'),
            pd.Timestamp('2024-01-01 12:00:03.250')
        ]

    def test_timestamp_column(self, session):
        timestamp_column = 'TEST'
        dataframe = self.get_test_data(timestamp_column=timestamp_column)

        src_schema = {
            'ID': int,
            'SIDE': str,
            'PRICE': float,
            'FLOAT': float,
            'SYMBOL_NAME': str,
        }
        _, compat = self.make_queries(
            dataframe, schema=src_schema, kwargs={'symbol': 'AAPL', 'timestamp_column': timestamp_column},
        )

        result_schema = {'Time', 'ID', 'SIDE', 'PRICE', 'FLOAT', 'SYMBOL_NAME'}
        assert len(compat) == 5
        assert set(compat.columns) == result_schema

    def test_empty_timestamp_column(self, session):
        timestamp_column = 'TEST'
        dataframe = self.get_test_data(timestamp_column=timestamp_column)

        src_schema = {
            timestamp_column: str,
            'ID': int,
            'SIDE': str,
            'PRICE': float,
            'FLOAT': float,
            'SYMBOL_NAME': str,
        }
        _, compat = self.make_queries(
            dataframe, schema=src_schema, kwargs={'symbol': 'AAPL'},
        )

        result_schema = {'Time', timestamp_column, 'ID', 'SIDE', 'PRICE', 'FLOAT', 'SYMBOL_NAME'}
        assert len(compat) == 5
        assert set(compat.columns) == result_schema

    def test_datetime_timestamp_column(self, session):
        timestamp_column = 'Timestamp'
        dataframe = self.get_test_data(timestamp_column=timestamp_column)
        dataframe[timestamp_column] = dataframe[timestamp_column].apply(pd.to_datetime)

        src_schema = {
            'ID': int,
            'SIDE': str,
            'PRICE': float,
            'FLOAT': float,
            'SYMBOL_NAME': str,
        }
        _, compat = self.make_queries(
            dataframe, schema=src_schema, kwargs={'symbol': 'AAPL'},
        )

        result_schema = {'Time', 'ID', 'SIDE', 'PRICE', 'FLOAT', 'SYMBOL_NAME'}
        assert len(compat) == 5
        assert set(compat.columns) == result_schema

    def test_timestamp_column_without_autodetect(self, session):
        timestamp_column = 'Timestamp'
        dataframe = self.get_test_data(timestamp_column=timestamp_column)

        # Also check fallback source multiple timestamp fields preserving
        dataframe['TimestamP'] = dataframe[timestamp_column]

        src_schema = {
            timestamp_column: str,
            'TimestamP': str,
            'ID': int,
            'SIDE': str,
            'PRICE': float,
            'FLOAT': float,
            'SYMBOL_NAME': str,
        }
        _, compat = self.make_queries(
            dataframe, schema=src_schema, kwargs={'symbol': 'AAPL', 'timestamp_column': None},
        )

        result_schema = {'Time', timestamp_column, 'TimestamP', 'ID', 'SIDE', 'PRICE', 'FLOAT', 'SYMBOL_NAME'}
        assert len(compat) == 5
        assert set(compat.columns) == result_schema

    def test_special_values_compat(self, session):
        dataframe = self.get_test_data(timestamp_column='TIME', add_special_values=True)

        src_schema = {
            'ID': int,
            'SIDE': str,
            'PRICE': float,
            'FLOAT': float,
            'SYMBOL_NAME': str,
        }
        _, compat = self.make_queries(
            dataframe, schema=src_schema, kwargs={'symbol': 'AAPL'}, drop_compare_for=['FLOAT'],
        )

        result_schema = {'Time', 'ID', 'SIDE', 'PRICE', 'FLOAT', 'SYMBOL_NAME'}
        assert len(compat) == 6
        assert set(compat.columns) == result_schema

    def test_symbol_name_field(self, session):
        dataframe = self.get_test_data(timestamp_column='Timestamp', symbol_name_field='TEST', symbol_name='AAPL')

        src_schema = {
            'ID': int,
            'SIDE': str,
            'PRICE': float,
            'FLOAT': float,
            'TEST': str,
        }
        _, compat = self.make_queries(dataframe, schema=src_schema, kwargs={'symbol_name_field': 'TEST'})

        result_schema = {'Time', 'ID', 'SIDE', 'PRICE', 'FLOAT', 'TEST'}
        assert len(compat) == 5
        assert set(compat.columns) == result_schema

    def test_exceptions(self, session):
        df = self.get_test_data(timestamp_column='Timestamp')

        with pytest.raises(ValueError, match='DataFrame should be passed'):
            otp.ReadFromDataFrame(symbol='AAPL')

        with pytest.raises(ValueError, match='expected to be pandas DataFrame'):
            otp.ReadFromDataFrame(dataframe={'A': [1, 2, 3]})

        for col in ['Time', 'TIMESTAMP']:
            df_ts = self.get_test_data(timestamp_column=col)
            df_ts['TEST'] = df_ts[col]
            with pytest.raises(ValueError, match='not allowed to both'):
                otp.ReadFromDataFrame(
                    self.get_test_data(timestamp_column=col), timestamp_column='TEST', symbol='AAPL',
                )

            with pytest.raises(ValueError, match='not allowed to both'):
                otp.ReadFromDataFrame(
                    self.get_test_data(timestamp_column=col), timestamp_column=None, symbol='AAPL',
                )

        df_ts = self.get_test_data(timestamp_column='TIMESTAMP')
        df_ts['TEST'] = df_ts['TIMESTAMP']
        with pytest.raises(ValueError, match='not allowed to both'):
            otp.ReadFromDataFrame(df_ts, timestamp_column='TEST', symbol='AAPL')

        df_ts = self.get_test_data(timestamp_column='TIMESTAMP')
        df_ts['TiMe'] = df_ts['TIMESTAMP']
        df_ts['Timestamp'] = df_ts['TIMESTAMP']
        with pytest.raises(ValueError, match='Could not determine timestamp'):
            otp.ReadFromDataFrame(df_ts, symbol='AAPL')

        with pytest.raises(ValueError, match='not in dataframe'):
            otp.ReadFromDataFrame(df, timestamp_column='MISSING', symbol='AAPL')

    def test_run(self, session):
        input_df = self.get_test_data()
        src = otp.ReadFromDataFrame(input_df)
        assert src.schema == {'ID': int, 'SIDE': str, 'PRICE': float, 'FLOAT': float, 'SYMBOL_NAME': str}
        df = otp.run(src, date=otp.dt(2024, 1, 1))
        assert list(df['Time']) == [otp.dt(2024, 1, 1, 12, 0, 0, 1000),
                                    otp.dt(2024, 1, 1, 12, 0, 2),
                                    otp.dt(2024, 1, 1, 12, 0, 2, 500000),
                                    otp.dt(2024, 1, 1, 12, 0, 3, 100000),
                                    otp.dt(2024, 1, 1, 12, 0, 3, 250000)]
        assert list(df['ID']) == [1, 1, 2, 2, 3]
        assert list(df['PRICE']) == [50.05, 50.05, 49.95, 49.98, 50.02]
        assert list(df['FLOAT']) == list(df['PRICE'])
        assert list(df['SYMBOL_NAME']) == ['AAPL', 'AAPL', 'AAPL', 'AAPL', 'AAPL']

    def test_copy(self, session):
        # PY-1484  (reproduces only with pyarrow installed)
        input_df = self.get_test_data()
        src = otp.ReadFromDataFrame(input_df)
        src = src.copy()
        assert src.schema == {'ID': int, 'SIDE': str, 'PRICE': float, 'FLOAT': float, 'SYMBOL_NAME': str}
        df = otp.run(src, date=otp.dt(2024, 1, 1))
        assert list(df['Time']) == [otp.dt(2024, 1, 1, 12, 0, 0, 1000),
                                    otp.dt(2024, 1, 1, 12, 0, 2),
                                    otp.dt(2024, 1, 1, 12, 0, 2, 500000),
                                    otp.dt(2024, 1, 1, 12, 0, 3, 100000),
                                    otp.dt(2024, 1, 1, 12, 0, 3, 250000)]
        assert list(df['ID']) == [1, 1, 2, 2, 3]
        assert list(df['PRICE']) == [50.05, 50.05, 49.95, 49.98, 50.02]
        assert list(df['FLOAT']) == list(df['PRICE'])
        assert list(df['SYMBOL_NAME']) == ['AAPL', 'AAPL', 'AAPL', 'AAPL', 'AAPL']

    def test_update(self, session):
        # PY-1484 (reproduces only with pyarrow installed)
        csv_input = dedent(
            """
            TIMESTAMP,STATE,ID,SIDE,PRICE,ORIG_QTY,LEAVES_QTY,FILL_QTY,FILL_PRICE
            2024-01-03 14:33:00.000,N,1,BUY,50.05,100,100,0,nan
            2024-01-03 14:33:02.000,PF,1,BUY,50.05,100,50,50,50.05
            2024-01-03 14:33:32.000,F,1,BUY,50.05,100,0,50,50.05
            2024-01-03 14:34:42.000,N,2,SELL,50.30,100,0,0,nan
            2024-01-03 14:34:52.000,C,2,SELL,50.30,100,0,0,nan
            2024-01-03 14:36:00.000,N,3,BUY,49.98,100,100,0,nan
            2024-01-03 14:36:02.000,PF,3,BUY,49.98,100,80,20,49.97
            2024-01-03 14:36:03.000,PF,3,BUY,49.98,100,60,20,49.97
            2024-01-03 14:36:03.250,PF,3,BUY,49.98,100,40,20,49.97
            2024-01-03 14:36:03.260,PF,3,BUY,49.98,100,20,20,49.97
            2024-01-03 14:36:04.000,PF,3,BUY,49.98,100,10,10,49.97
            2024-01-03 14:36:12.000,F,3,BUY,49.98,100,0,10,49.95
            """
        )
        # CSV with TIMESTAMP field set to %Y%m%d%H%M%S.%f format in UTC
        df = pd.read_csv(StringIO(csv_input))
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])

        # Load DataFrame as a source, specifying the symbol
        orders = otp.ReadFromDataFrame(df, symbol='AAPL', db='LOCAL')
        orders['NEW_QTY'] = orders['ORIG_QTY']
        orders = orders.update(if_set={orders['NEW_QTY']: orders['LEAVES_QTY']}, where=(orders['STATE'] == 'PF'))

        # Run the query for a specific date
        result = otp.run(orders, date=otp.date(2024, 1, 3))
        assert list(result['STATE']) == ['N', 'PF', 'F', 'N', 'C', 'N', 'PF', 'PF', 'PF', 'PF', 'PF', 'F']
        assert list(result['ORIG_QTY']) == [100] * 12
        assert list(result['LEAVES_QTY']) == [100, 50, 0, 0, 0, 100, 80, 60, 40, 20, 10, 0]
        assert list(result['NEW_QTY']) == [100, 50, 100, 100, 100, 100, 80, 60, 40, 20, 10, 100]
