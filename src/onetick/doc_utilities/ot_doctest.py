import doctest
import re
import inspect

from typing import Union, Optional


def snippet_name(example: Union[doctest.Example, str], value):
    if isinstance(example, doctest.Example):
        example.name = value    # type: ignore
    return example


def skip_snippet(example: Union[doctest.Example, str], value):
    if isinstance(example, doctest.Example):
        example.skip_snippet = True    # type: ignore
    return example


def skip_example(example, value, caller=None):
    if caller is not None and value != '' and value != caller:  # do nothing if caller passed and value not caller
        return example
    if isinstance(example, doctest.Example):
        example.skip = True
        return example
    return None


OT_DIRECTIVES = {'skip-example': skip_example,
                 'snippet-name': snippet_name,
                 'skip-snippet': skip_snippet}


def register_ot_directive(directive, fun):
    """
    Register OT directive.

    directive sintax: ...    OTdirective: <directive>: <directive_parameter>;

    Parameters
    ----------
    directive: str
        directive name
    fun: callable
        fun will be executed if `directive` will be found
        fun should have two positional parameters:
            first: string or doctest.Example
            second: direction parameter
        fun should return str, doctest.Example or None
    """
    OT_DIRECTIVES[directive] = fun


class ApplyDirective:

    _OT_DIRECTIVE_RE = r'(.*?)\s*# OTdirective: (.*)'
    _OT_DIRECTIVE_PARSER = re.compile(r'\s*?(?P<func>[A-Za-z_\-0-9]*)\s*?:(?P<param>[^:;]*);')

    def __init__(self, caller: Optional[str] = None):
        self.caller = caller

    def _get_directives(self, item):
        is_example = isinstance(item, doctest.Example)
        string = item
        if is_example:
            string = item.source
        res = []
        funcs = []
        for doc in string.split('\n'):
            s, f = self._get_directives_impl(doc)
            res.append(s)
            funcs.extend(f)

        string = '\n'.join(res)
        if is_example:
            item.source = string
            return item, funcs
        return string, funcs

    def _get_directives_impl(self, string: str):
        directives = re.match(self._OT_DIRECTIVE_RE, string, re.MULTILINE | re.DOTALL)
        if directives is None:
            return string, {}
        doc, directive_str = directives.groups()
        funcs = []
        for fun, v in re.findall(self._OT_DIRECTIVE_PARSER, directive_str):
            if fun.strip() not in OT_DIRECTIVES:
                raise KeyError(f"Unknown directive: '{fun.strip()}'. Original: '{directive_str}'")
            funcs.append((OT_DIRECTIVES[fun.strip()], v.strip()))
        return doc, funcs

    def __call__(self, item):
        doc, funcs = self._get_directives(item)
        for fun, param in funcs:
            params = {}
            if self.caller and 'caller' in inspect.signature(fun).parameters.keys():
                params['caller'] = self.caller
            doc = fun(doc, param, **params)
        return doc


def apply_directive(example: Union[doctest.Example, str], caller: Optional[str] = None):

    """
    Applies OT directive to `example`
    Drops directive comment

    See Also
    --------
    register_ot_directive
    """

    return ApplyDirective(caller)(example)


class OTDoctestParser(doctest.DocTestParser):

    """
    This class applicable only to parse doctest example!
    Do not run tests with this parser!
    """

    def __init__(self, *args, caller: Optional[str] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.caller = caller

    def _IS_BLANK_OR_COMMENT(self, value):

        if value.strip().startswith('# OTdirective'):
            return False
        else:
            return super()._IS_BLANK_OR_COMMENT(value)

    def parse(self, *args, **kwargs):
        """
        Applies OT directive for all examples
        """
        examples = super().parse(*args, **kwargs)
        res = [apply_directive(x, self.caller) for x in examples]
        return res
