import os
import re
import inspect
from typing import TYPE_CHECKING
from inspect import Parameter, formatannotation, Signature
from functools import wraps
from collections import defaultdict

from onetick.py.docs.docstring_parser import Docstring

if TYPE_CHECKING:
    from onetick.py.core.source import Source


def param_doc(name,
              str_annotation=None,
              str_default=None,
              desc=None,
              annotation=Parameter.empty,
              default=Parameter.empty,
              kind=Parameter.POSITIONAL_OR_KEYWORD) -> tuple[str, Parameter]:
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


def _merge_sig_params(sig_params: list[Parameter], doc_params: list[str | Parameter]) -> list[Parameter]:
    """
    Merge real object signature with parameters from the docstring.
    Parameters from the docstring will be specified as KEYWORD_ONLY
    if they do not have kind specified and if they do not exist in object signature.
    """
    sig_params_names = [param.name for param in sig_params]
    doc_sig_params = []
    for doc_param in doc_params:
        if isinstance(doc_param, Parameter):
            if doc_param.name not in sig_params_names:
                doc_sig_params.append(doc_param)
        else:
            if doc_param not in sig_params_names:
                doc_sig_params.append(Parameter(name=doc_param, kind=Parameter.KEYWORD_ONLY))

    doc_kinds = defaultdict(list)
    for param in doc_sig_params:
        doc_kinds[param.kind].append(param)

    sig_kinds = defaultdict(list)
    for param in sig_params:
        sig_kinds[param.kind].append(param)

    result = []
    for kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD,
                 Parameter.VAR_POSITIONAL, Parameter.KEYWORD_ONLY, Parameter.VAR_KEYWORD):
        result.extend(sig_kinds[kind])
        result.extend(doc_kinds[kind])
    return result


def docstring(*, parameters: list, add_self=False, use_obj_signature=True):
    """
    Modify object's docstring and signature by *adding* parameters described in ``parameters`` list.
    Object can be a class or a function.
    """
    def _decorator(obj):
        doc = obj.__doc__ or ''
        doc = Docstring(doc)

        doc_parameters = doc.get_parameter_names()

        # get parameters from the obj
        if use_obj_signature is not None:
            sig_params = _merge_sig_params(inspect.signature(obj).parameters.values(), doc_parameters)
        else:
            sig_params = []
            if add_self:
                sig_params.append(Parameter(name='self', kind=Parameter.POSITIONAL_OR_KEYWORD))
            sig_params.extend([Parameter(name=name, kind=Parameter.POSITIONAL_OR_KEYWORD) for name in doc_parameters])

        # add parameters from decorator
        for d, _ in parameters:
            doc['Parameters'] = d
        sig_params = _merge_sig_params(sig_params, [param for _, param in parameters])

        # replace docstring and signature
        obj.__doc__ = doc.build()
        obj.__signature__ = Signature(sig_params)
        return obj
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
