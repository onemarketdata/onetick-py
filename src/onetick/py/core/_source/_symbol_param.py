import onetick.py as otp
import onetick.py.types as ott
from onetick.py.core.column import _Column
from onetick.py.core.column_operations.base import _Operation


class _SymbolParamColumn(_Column):
    """
    Internal object representing OneTick's symbol parameters.
    Can be used in other onetick.py methods to specify if object is symbol parameter.
    """

    def __init__(self, *args, default=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._default = default

    def __str__(self):
        result = super().__str__()

        # TODO: PY-35
        # This is ad-hoc check, really we need to change column formatting to
        # pre- and post-formats, and copy columns through the .copy() method
        # on the _Column instead of copying them manually in different places
        # of the _Source class
        if self.name != '_SYMBOL_NAME':
            result = f'_SYMBOL_PARAM.{self.name}'

        if self._default is not None:
            default_formatted = ott.value2str(self._default)
            default_formatted = f'TOSTRING({default_formatted})'
            result = f'CASE(UNDEFINED("{result}"), false, {result}, {default_formatted})'

        # symbol params are always string, need to convert
        result = _Operation(op_str=result, dtype=str)

        if self.dtype is ott.nsectime:
            result = result.str.to_datetime(unit='ns')
        elif self.dtype is ott.msectime:
            result = result.str.to_datetime(unit='ms')
        else:
            result = result.astype(self.dtype)

        return str(result)


class _SymbolParamSource:
    """
    Internal container that provides access to symbol parameters.
    The object is read-only, you can only get symbol parameters with it.
    """

    def __init__(self, **columns):

        if 'name' in columns:
            raise ValueError(
                'name field is a specific field for accessing _SYMBOL_NAME variable, please rename your parameter'
            )

        # "SYMBOL_NAME" field won't be propagated as a symbol parameter by onetick, so we should remove it here
        columns.pop('SYMBOL_NAME', None)

        for key, dtype in columns.items():
            # TODO: change to immutable Column, PY-35
            self.__dict__[key] = _SymbolParamColumn(key, dtype, self)

        self.__dict__['name'] = _SymbolParamColumn('_SYMBOL_NAME', str, self)
        self.__dict__['_SYMBOL_NAME'] = self.__dict__['name']

        # set schema
        schema = columns.copy()

        for meta_c in otp.meta_fields.get_onetick_fields_and_types():
            schema.pop(meta_c, None)

        self.__dict__['_schema'] = schema

    @property
    def schema(self):
        """
        Represents actual python data schema in the column name -> type format.
        """
        return self._schema

    def __getitem__(self, key):
        """
        Get symbol parameter with corresponding ``key``.
        Raises an error if such column does not exist.
        """
        if key not in self.__dict__:
            raise AttributeError(f"There is no '{key}' column")

        return self.__dict__[key]

    def __setattr__(self, key, value):
        raise NotImplementedError('Symbol params are read only')
