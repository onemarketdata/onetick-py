from typing import Dict
import collections.abc


class Schema(collections.abc.Mapping):
    """
    A source data schema proxy. It allows to work with source schema in
    a frozen-dict manner. It also allows to set using the ``set``
    and update a source schema using the ``update`` methods.
    """

    dict_cls = dict

    def __init__(self, _base_source=None, _hidden_columns=None, **kwargs):
        self._dict = self.dict_cls(**kwargs)
        self._base_source = _base_source
        self._hash = None
        self._hidden_columns = _hidden_columns if _hidden_columns else {}

    def __getitem__(self, item):
        if isinstance(item, list):
            return {key: value for key, value in self._dict.items()
                    if key in item}

        if item in self._hidden_columns:
            return self._hidden_columns[item]

        return self._dict[item]

    def __setitem__(self, key, value):
        self.update(**{key: value})

    def __contains__(self, item):
        if item in self._hidden_columns:
            return True

        return item in self._dict

    def __iter__(self):
        return iter(self._dict)

    def items(self):
        return self._dict.items()

    def keys(self):
        return self._dict.keys()

    def copy(self):
        return self._dict.copy()

    def set(self, **new_schema: type):
        """
        Drops the python schema representation of a source and sets the new one from the `new_schema`

        Parameters
        ----------
        new_schema: Dict[str, type]
            schema in the column-name -> type format

        Returns
        -------
            None
        """
        self._base_source.set_schema(**new_schema)

    def update(self, **other_schema: type):
        """
        Updates the python schema representation of a source: values from matching keys
        will be overridden from the `other_schema`, values from new keys will be added.

        Parameters
        ----------
        other_schema:
            schema in the column-name -> type format

        Returns
        -------
            None
        """
        current_schema = self._base_source.columns(skip_meta_fields=True)
        current_schema.update(other_schema)

        self._base_source.set_schema(**current_schema)

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        return repr(self._dict)

    def __hash__(self):
        if self._hash is None:
            h = 0
            for key, value in self._dict.items():
                h ^= hash((key, value))
            self._hash = h
        return self._hash
