# A common place to put backports for specific versions of Python

import sys

if sys.version_info >= (3, 8):
    from typing import Literal
    from functools import singledispatchmethod, lru_cache
else:
    from typing_extensions import Literal
    from singledispatchmethod import singledispatchmethod
    from backports.functools_lru_cache import lru_cache


if sys.version_info >= (3, 9):
    import zoneinfo
    from functools import cached_property, cache
    import ast
    astunparse = ast.unparse
else:
    from backports.cached_property import cached_property
    from astunparse import unparse as astunparse
    from backports import zoneinfo

    def cache(user_function):
        'Simple lightweight unbounded cache.  Sometimes called "memoize".'
        return lru_cache(maxsize=None)(user_function)
