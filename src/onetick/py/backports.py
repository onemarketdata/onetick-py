# A common place to put backports for specific versions of Python

import sys
from typing import TYPE_CHECKING, Any

# this allows to not import typing_extensions module on runtime
# thus it's not required to add it to main dependencies, only to dev
if TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        # https://pypi.org/project/typing-extensions/
        from typing_extensions import Self
else:
    # otherwise just allow to import some dummy value
    Self = Any
