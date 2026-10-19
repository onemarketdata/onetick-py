from typing import Optional

import onetick.py as otp
from onetick.py.otq import otq
from .misc import inplace_operation


@inplace_operation
def add_fields_from_symbol_params(
    self: 'otp.Source',
    /,
    *,
    prefix: str | None = None,
    inplace=False,
) -> Optional['otp.Source']:
    """
    Modifies the input time series by adding all symbol parameters
    (preserving original value types) as new fields to each tick.

    .. note::

        This OneTick EP returns arbitrary number of fields,
        so the source's :attr:`~onetick.py.Source.schema` will not be updated automatically.

    Parameters
    ----------
    prefix: str
        If specified, it will be appended to each added tick field name with the following form:
        ``PREFIX.SYMBOL_PARAM_NAME``

    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.

    Returns
    -------
    :class:`Source` or ``None``

    See Also
    --------
    **ADD_FIELDS_FROM_SYMBOL_PARAMS** OneTick event processor

    Examples
    --------

    Let's create first-stage query with some symbol parameters first:

    >>> symbols = otp.Tick(SYMBOL_NAME='AAPL', PARAM_1='HELLO', PARAM_2=12345, PARAM_3=1.2345)

    Then add these symbol parameters as new fields:

    >>> data = otp.Tick(A=1)
    >>> data = data.add_fields_from_symbol_params()
    >>> data = otp.merge([data], symbols=symbols)
    >>> otp.run(data)
            Time  A PARAM_1  PARAM_2  PARAM_3
    0 2003-12-01  1   HELLO    12345   1.2345

    Note that source's :attr:`~onetick.py.Source.schema` can't be updated automatically in this case
    and need to be set manually in order to use new fields:

    >>> data.schema.update(**{'PARAM_1': str, 'PARAM_2': int, 'PARAM_3': float})
    >>> data['B'] = data['PARAM_2'] + 1
    >>> otp.run(data)
            Time  A PARAM_1  PARAM_2  PARAM_3      B
    0 2003-12-01  1   HELLO    12345   1.2345  12346

    Parameter ``prefix`` can be used to add some prefix to the new fields:

    >>> data = otp.Tick(A=1)
    >>> data = data.add_fields_from_symbol_params(prefix='PREFIX')
    >>> data = otp.merge([data], symbols=symbols)
    >>> otp.run(data)
            Time  A PREFIX.PARAM_1  PREFIX.PARAM_2  PREFIX.PARAM_3
    0 2003-12-01  1          HELLO           12345          1.2345
    """
    ep_kwargs = {}
    if prefix is not None:
        ep_kwargs['prefix'] = prefix

    ep = otq.AddFieldsFromSymbolParams(**ep_kwargs)
    self.sink(ep)
    return self


@inplace_operation
def mid(
    self: 'otp.Source',
    /,
    output_field_name: str = 'MID',
    *,
    inplace=False,
) -> Optional['otp.Source']:
    """
    Adds to the stream of quote ticks a numeric attribute
    whose value for each tick is equal to the mid-quote: ``MID = (BID_PRICE + ASK_PRICE) / 2``.

    Parameters
    ----------
    output_field_name: str
        The name of the output field.
    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.

    Returns
    -------
    :class:`Source` or ``None``

    See Also
    --------
    **MID** OneTick event processor

    Examples
    --------

    Add field **MID** to the output:

    >>> data = otp.DataSource('US_COMP_SAMPLE', tick_type='QTE', symbols='AAPL')
    >>> data = data[['ASK_PRICE', 'BID_PRICE']]
    >>> data = data.mid()
    >>> data = data.limit(100)
    >>> otp.run(data, date=otp.dt(2024, 2, 1), timezone='America/New_York')  # doctest: +ELLIPSIS
                                Time  ASK_PRICE  BID_PRICE      MID
    0  2024-02-01 03:59:00.048766251       0.00       0.00    0.000
    1  2024-02-01 03:59:00.076961302       0.00       0.00    0.000
    2  2024-02-01 04:00:00.004396668     187.02     184.87  185.945
    3  2024-02-01 04:00:00.004557945     187.02     184.87  185.945
    4  2024-02-01 04:00:00.004697438     187.02     184.87  185.945
    ..                           ...        ...        ...      ...

    Parameter ``output_field_name`` can be used to change the name of the output field:

    >>> data = otp.DataSource('US_COMP_SAMPLE', tick_type='QTE', symbols='AAPL')
    >>> data = data[['ASK_PRICE', 'BID_PRICE']]
    >>> data = data.mid('MID_PRICE')
    >>> data = data.limit(100)
    >>> otp.run(data, date=otp.dt(2024, 2, 1), timezone='America/New_York')  # doctest: +ELLIPSIS
                                Time  ASK_PRICE  BID_PRICE  MID_PRICE
    0  2024-02-01 03:59:00.048766251       0.00       0.00      0.000
    1  2024-02-01 03:59:00.076961302       0.00       0.00      0.000
    2  2024-02-01 04:00:00.004396668     187.02     184.87    185.945
    3  2024-02-01 04:00:00.004557945     187.02     184.87    185.945
    4  2024-02-01 04:00:00.004697438     187.02     184.87    185.945
    ..                           ...        ...        ...        ...
    """
    for field in ('ASK_PRICE', 'BID_PRICE'):
        if field not in self.schema:
            raise ValueError(f"There is no '{field}' column in the schema")

    if output_field_name in self.schema:
        raise ValueError(f"Column '{output_field_name}' already exists in the schema")

    ep = otq.Mid(output_field_name=output_field_name)
    self.sink(ep)
    self.schema[output_field_name] = float
    return self


@inplace_operation
def interpolate(
    self: 'otp.Source',
    /,
    interval: int,
    fields: str | list[str] | dict[str, str],
    method: str = 'linear',
    *,
    inplace=False,
) -> Optional['otp.Source']:
    """
    Compute a series of interpolated ticks at regular intervals.

    Parameters
    ----------
    method: str
        When set to ``linear`` (default), linear interpolation is performed.
        When set to ``step``, the last non-null value is taken as the interpolation result.
    interval: int
        The interval in seconds to interpolate.
    fields: str or list or dict
        A field name or a list of field names to interpolate, same field names are used in the output.
        If dictionary is passed, it is treated as mapping from input to output field names.
    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.

    Returns
    -------
    :class:`Source` or ``None``

    See Also
    --------
    **INTERPOLATE** OneTick event processor

    Examples
    --------
    10-second linear interpolation of *ASK_PRICE* and *BID_PRICE*:

    >>> data = otp.DataSource('US_COMP_SAMPLE', tick_type='QTE', symbols='AAPL')
    >>> data = data.interpolate(10, ['ASK_PRICE', 'BID_PRICE'])
    >>> data = data.limit(100)
    >>> otp.run(data, date=otp.dt(2024, 2, 1), timezone='America/New_York')  # doctest: +ELLIPSIS
                      Time   ASK_PRICE   BID_PRICE
    0  2024-02-01 03:59:10   30.970272   30.614235
    1  2024-02-01 03:59:20   62.177721   61.462920
    2  2024-02-01 03:59:30   93.385170   92.311605
    3  2024-02-01 03:59:40  124.592619  123.160290
    4  2024-02-01 03:59:50  155.800068  154.008975
    ..                 ...         ...         ...

    Use dictionary as ``fields`` parameter to specify output field names:

    >>> data = otp.DataSource('US_COMP_SAMPLE', tick_type='QTE', symbols='AAPL')
    >>> data = data.interpolate(10, {'ASK_PRICE': 'ASK', 'BID_PRICE': 'BID'})
    >>> data = data.limit(100)
    >>> otp.run(data, date=otp.dt(2024, 2, 1), timezone='America/New_York')  # doctest: +ELLIPSIS
                      Time         ASK         BID
    0  2024-02-01 03:59:10   30.970272   30.614235
    1  2024-02-01 03:59:20   62.177721   61.462920
    2  2024-02-01 03:59:30   93.385170   92.311605
    3  2024-02-01 03:59:40  124.592619  123.160290
    4  2024-02-01 03:59:50  155.800068  154.008975
    ..                 ...         ...         ...
    """
    if isinstance(fields, str):
        fields = {fields: fields}
    elif not isinstance(fields, dict):
        fields = dict(zip(fields, fields))

    for field in fields:
        if field not in self.schema:
            raise ValueError(f"There is no '{field}' column in the schema")

    input_fields_str = ','.join(fields)
    output_fields_str = ','.join(fields.values())

    ep = otq.Interpolate(input_field_names=input_fields_str,
                         output_field_names=output_fields_str,
                         interval=interval,
                         method=method.upper())
    self.sink(ep)
    self.schema.set(**{
        output_name: self.schema[input_name]
        for input_name, output_name in fields.items()
    })
    return self


@inplace_operation
def encoding_converter(
    self: 'otp.Source',
    input_encoding: str,
    output_encoding: str,
    /,
    fields: str | list[str] | None = None,
    *,
    inplace=False,
) -> Optional['otp.Source']:
    """
    Converts the values of selected target string or :class:`~onetick.py.types.varstring`
    fields in each input tick to a specified ``output_encoding``,
    with the converted values output in :class:`~onetick.py.types.varstring` format.

    .. note::

        Popular supported encoding names are: ASCII, UTF-8, UTF-16, UTF-32, Latin-1, Windows-1252, MacRoman, ISO 8859-1.
        For the full list of existing string encoding names, check the IANA Character Sets.

    Parameters
    ----------
    fields: str or list
        A field name or a list of field names to convert.
        If empty, then all string fields will be converted.
    input_encoding: int
        The encoding format for the input data.
    output_encoding: int
        The encoding format for the output data.
    inplace : bool
        The flag controls whether operation should be applied inplace or not.
        If ``inplace=True``, then it returns nothing. Otherwise method returns a new modified
        object.

    Returns
    -------
    :class:`Source` or ``None``

    See Also
    --------
    **ENCODING_CONVERTER** OneTick event processor

    Examples
    --------

    Convert all string fields from UTF-8 to ASCII encoding:

    >>> data = otp.Tick(A='español')
    >>> data = data.encoding_converter('utf-8', 'ascii')
    >>> otp.run(data, encoding='ascii')
            Time        A
    0 2003-12-01  espa\x1aol

    Or convert field A from UTF-8 to Latin-1:

    >>> data = otp.Tick(A='español 🤔')
    >>> data = data.encoding_converter('utf-8', 'Latin-1', ['A'])
    >>> otp.run(data, encoding='Latin-1')
            Time          A
    0 2003-12-01  español \x1a
    """
    if fields:
        if isinstance(fields, str):
            fields = [fields]

        for field in fields:
            if field not in self.schema:
                raise ValueError(f"There is no '{field}' column in the schema")
            dtype = self.schema[field]
            if not issubclass(dtype, otp.string) and dtype is not str:
                raise TypeError(f"'{field}' column is not if string type")

        fields_str = ','.join(fields)
    else:
        fields_str = ''

    ep = otq.EncodingConverter(
        fields=fields_str,
        input_encoding=input_encoding,
        output_encoding=output_encoding,
    )
    self.sink(ep)

    if fields:
        schema = {field: otp.varstring for field in fields}
    else:
        schema = {
            field: otp.varstring
            for field, dtype in self.schema.items()
            if issubclass(dtype, otp.string) or dtype is str
        }
    # use table to remove null characters
    self.table(**schema, strict=False, inplace=True)
    return self
