from onetick.py.otq import otq


class SqlQuery(otq.SqlQuery):
    def __init__(self, sql_statement: str, merge_all_symbols: bool = False, separate_dbname: bool = False):
        """
        Constructs SQL query object.

        Parameters
        ----------
        sql_statement:
            The SQL statement string.
        merge_all_symbols:
            If set to True, ticks returned by the query for all symbols get merged into a single time series.
        separate_dbname:
             If set to True, and ``merge_all_symbols`` is set to True,
             *SYMBOL_NAME* field contains a symbol name without the database name,
             and *DB_NAME* field contains the database name for a symbol.

        See also
        --------
        :py:func:`otp.run <onetick.py.run>`

        Examples
        --------

        Select two fields from a single tick type and symbol and return first three ticks from a single day:

        >>> otp.run(
        ...     otp.SqlQuery("select PRICE,SIZE from US_COMP.TRD"
        ...                  " where symbol_name = 'AAPL'"
        ...                  " and start_time = '2022-03-01 00:00:00 GMT' and end_time = '2022-03-02 00:00:00 GMT'"
        ...                  " limit 3")
        ... )
                             Time  PRICE  SIZE
        0 2022-03-01 00:00:00.000    1.3   100
        1 2022-03-01 00:00:00.001    1.4    10
        2 2022-03-01 00:00:00.002    1.4    50

        Join quotes and trades:

        >>> otp.run(
        ...     otp.SqlQuery("select t.PRICE,q.ASK_PRICE,q.BID_PRICE"
        ...                  " from US_COMP.TRD t join US_COMP.QTE q"
        ...                  " on sametime_as_existing(t.timestamp, q.timestamp, 0) = TRUE"
        ...                  " where t.symbol_name = 'AAPL' and q.symbol_name = 'AAPL'"
        ...                  " and start_time = '2022-03-01 00:00:00 GMT' and end_time = '2022-03-02 00:00:00 GMT'"
        ...                  " limit 2")
        ... )
                             Time  T.PRICE  Q.ASK_PRICE  Q.BID_PRICE
        0 2022-03-01 00:00:00.001      1.4          1.5          1.2
        1 2022-03-01 00:00:00.002      1.4          1.4          1.3

        Calculate average price of trades across several symbols:

        >>> otp.run(
        ...     otp.SqlQuery("select COUNT(*) as COUNT, AVG(PRICE) as AVG_PRICE"
        ...                  " from US_COMP.TRD"
        ...                  " where symbol_name in ('AAPL', 'AAP')"
        ...                  " and start_time = '2022-03-01 00:00:00 GMT' and end_time = '2022-03-02 00:00:00 GMT'",
        ...                  merge_all_symbols=True)
        ... )
                         Time  COUNT  AVG_PRICE
        0 2022-03-01 19:00:00    5.0     18.976
        """
        super().__init__(sql_statement)
        if merge_all_symbols:
            self.set_merge_all_symbols_flag(merge_all_symbols)
        if separate_dbname:
            self.set_separate_dbname_flag(separate_dbname)
