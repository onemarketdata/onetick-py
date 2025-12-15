from io import StringIO

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
    def test_timestamp_column_auto(self, f_session, timestamp_column):
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

    def test_timestamp_column(self, f_session):
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

    def test_empty_timestamp_column(self, f_session):
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

    def test_datetime_timestamp_column(self, f_session):
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

    def test_timestamp_column_without_autodetect(self, f_session):
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

    def test_special_values_compat(self, f_session):
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

    def test_symbol_name_field(self, f_session):
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

    def test_exceptions(self):
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
