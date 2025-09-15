import os
import re
from typing import Tuple, Optional, TYPE_CHECKING
from inspect import Parameter, formatannotation, Signature
from functools import wraps

from onetick.py.docs.docstring_parser import Docstring

if TYPE_CHECKING:
    from onetick.py.core.source import Source


def param_doc(name,
              str_annotation=None,
              str_default=None,
              desc=None,
              annotation=Parameter.empty,
              default=Parameter.empty,
              kind=Parameter.POSITIONAL_OR_KEYWORD) -> Tuple[str, Parameter]:
    doc = f"""{name}"""

    if str_annotation is None and annotation is not Parameter.empty:
        str_annotation = formatannotation(annotation)

    if str_annotation:
        doc += f": {str_annotation}"
        if str_default:
            doc += f", default={str_default}"
        elif default is not Parameter.empty:
            doc += f", default={default}"
    if desc:
        doc += f"    {desc}"
    param = Parameter(name=name,
                      kind=kind,
                      default=default,
                      annotation=annotation)
    return doc, param


def docstring(parameters: Optional[list] = None, add_self=False):
    parameters = parameters or []

    def _decorator(fun):
        @wraps(fun)
        def _inner(*args, **kwargs):
            return fun(*args, **kwargs)
        doc = fun.__doc__ or ''
        doc = Docstring(doc)
        sig = []
        if add_self:
            sig.append(Parameter(name='self', kind=Parameter.POSITIONAL_OR_KEYWORD))
        sig.extend(doc.get_sig())
        for d, param in parameters:
            doc['Parameters'] = d
            sig.append(param)
        _inner.__doc__ = doc.build()
        _inner.__signature__ = Signature(sig)
        return _inner
    return _decorator


def alias(aliased_fun, doc_replacer=None, skip_ot_directives=True):
    """
    Returns new function with docstring and
    signature copied from ``aliased_fun``.
    """

    @wraps(aliased_fun)
    def fun(*args, **kwargs):
        return aliased_fun(*args, **kwargs)

    if doc_replacer:
        fun.__doc__ = doc_replacer(fun.__doc__)
    if skip_ot_directives:
        fun.__doc__ = re.sub('# OTdirective: .*', '', fun.__doc__)
    return fun


def is_windows():
    # PY-866: needed to be used in :skipif: sphinx doctest directives
    return os.name == 'nt'
