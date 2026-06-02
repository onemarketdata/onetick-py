# mypy: disable-error-code="assignment"

import dataclasses
from abc import ABC, abstractmethod

import onetick.py.types as ott
from onetick.py.backports import Self


class Validator(ABC):
    # https://docs.python.org/3/library/dataclasses.html#descriptor-typed-fields

    def __init__(self, default_value):
        self._default = default_value

    def __set_name__(self, owner, name):
        self._name = '_' + name

    def __get__(self, instance, owner=None):
        if instance is None:
            return self._default
        return getattr(instance, self._name, self._default)

    def __set__(self, instance, value):
        if value is self._default:
            new_value = value
        else:
            new_value = self.validate(value)
        setattr(instance, self._name, new_value)

    @abstractmethod
    def validate(self, value):
        pass


class DatetimeDescriptor(Validator):
    def validate(self, value):
        return ott.datetime(value)


class BoolDescriptor(Validator):
    def validate(self, value):
        return bool(value)


class IntDescriptor(Validator):
    def validate(self, value):
        return int(value)


class DictDescriptor(Validator):
    def validate(self, value):
        return dict(value)


class StrDescriptor(Validator):
    def validate(self, value):
        return str(value)


# this class is PUBLIC
@dataclasses.dataclass(kw_only=True)
class QueryParameters:
    """
    OneTick queries have different properties.

    They can be set separately for each query in the resulting .otq file.

    Some of them have separate setters and can be specified as separate fields here,
    others can be specified with ``query_properties`` dictionary.

    Not all properties are specified here, some of them may be set in different places:

        * some options in :class:`otp.config <onetick.py.configuration.Config>`
        * some parameters in :class:`otp.DataSource <onetick.py.DataSource>` and other source classes
        * some parameters in :py:func:`otp.run <onetick.py.run>`
        * etc.

    If the query properties are set in different places at the same time,
    the value specified last will take precedence.

    The default value for all fields here is None, which means that field is not set.

    Examples
    --------

    Set the query property when creating source object:

    >>> t = otp.Tick(A=1, query_parameters=otp.QueryParameters(query_properties={'ALLOW_GRAPH_REUSE': 'TRUE'}))
    >>> t['ALLOW_GRAPH_REUSE'] = otp.get_query_property('ALLOW_GRAPH_REUSE')
    >>> otp.run(t)
            Time  A ALLOW_GRAPH_REUSE
    0 2003-12-01  1              TRUE

    The property can be overridden when running the query:

    >>> otp.run(t, query_properties={'ALLOW_GRAPH_REUSE': 'FALSE'})
            Time  A ALLOW_GRAPH_REUSE
    0 2003-12-01  1             FALSE
    """

    symbol_date: ott.datetime | None = DatetimeDescriptor(None)

    concurrency: int | None = IntDescriptor(None)
    batch_size: int | None = IntDescriptor(None)

    running: bool | None = BoolDescriptor(None)

    query_properties: dict | None = DictDescriptor(None)

    def __setattr__(self, name, value):
        fields = self.field_names()
        if not name.startswith('_') and name not in fields:
            raise ValueError(f"Can't set field {repr(name)}, only these fields can be set: {fields}")
        return super().__setattr__(name, value)

    def field_names(self) -> list[str]:
        return [f.name for f in dataclasses.fields(self)]

    def asdict(self) -> dict:
        return dataclasses.asdict(self)

    def merge(self, other: Self | None) -> Self:
        """
        Merge two sets of query parameters.
        If some fields are set in both objects, then the fields from ``other`` will take precedence.
        Doesn't modify ``self`` or ``other``, returns a new object.
        """
        kwargs = {}
        for field in self.field_names():
            our_value = getattr(self, field)
            if other is None:
                other_value = None
            else:
                other_value = getattr(other, field, None)
            if other_value is not None:
                kwargs[field] = other_value
            elif our_value is not None:
                kwargs[field] = our_value
        return self.__class__(**kwargs)

    def copy(self) -> Self:
        return self.__class__(**self.asdict())


@dataclasses.dataclass(kw_only=True)
class _ExtendedQueryParameters(QueryParameters):
    # For now intended only for internal use.
    # At least start, end and symbols fields are very special and are calculated separately for otp.Source objects.
    # Big refactoring is needed until we can make this class public.

    start: ott.datetime | None = None
    end: ott.datetime | None = None
    timezone: str | None = None

    start_time_expression: str | None = None
    end_time_expression: str | None = None

    symbols: list[str] | None = None
