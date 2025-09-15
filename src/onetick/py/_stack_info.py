import os
import traceback
from typing import List

from .configuration import config


# dictionary to save traceback lines
_TRACE_LINES = {}
# dictionary to save traceback tuples
_TRACE_TUPLES = {}


def _get_id_with_traceback(tb_list: List[str]) -> str:
    """
    Save traceback in some memory-efficient way
    and return unique id of the traceback.
    """
    trace_list = []
    for line in tb_list:
        if line not in _TRACE_LINES:
            # using the same string objects for different tracebacks
            _TRACE_LINES[line] = line
        trace_list.append(_TRACE_LINES[line])

    trace_tuple = tuple(trace_list)
    trace_hash = str(hash(trace_tuple))
    if trace_hash not in _TRACE_TUPLES:
        # using the same tuple objects for different tracebacks
        _TRACE_TUPLES[trace_hash] = trace_tuple
    return trace_hash


def _get_traceback_with_id(trace_hash: str) -> str:
    """
    Get our custom saved traceback from dictionary by hash.
    """
    return ''.join(_TRACE_TUPLES[trace_hash])


def _modify_stack_info_in_onetick_query():
    """
    Change stack_info parameter in all OneTick's event processors.
    Save full traceback instead of filename + line number.
    """

    def modify_init(cls):
        old_init = cls.__init__

        def new_init(self, *args, **kwargs):
            old_init(self, *args, **kwargs)
            if config.show_stack_info and hasattr(self, 'stack_info'):
                self.stack_info = _get_id_with_traceback(traceback.format_stack()[:-1])

        cls.__init__ = new_init

    from onetick.py.otq import otq
    eps_classes = (
        cls for cls in otq.__dict__.values()
        if isinstance(cls, type) and issubclass(cls, otq.EpBase) and cls is not otq.EpBase
    )
    for ep_cls in eps_classes:
        modify_init(ep_cls)


def _add_stack_info_to_exception(exc):
    """
    Find stack_info parameter in OneTick exception message.
    Get stack trace and append it to the passed exception.
    """
    if not config.show_stack_info:
        return exc

    _, _, location_details = str(exc).partition('Problem location details:')
    stack_info_uuid = None
    for block in location_details.strip().split(','):
        name, _, value = block.partition('=')
        if name == 'stack_info':
            stack_info_uuid = value
            break

    if not stack_info_uuid:
        return exc

    stack_info = _get_traceback_with_id(stack_info_uuid)

    if exc.args:
        exc.args = (exc.args[0] + os.linesep + stack_info, *exc.args[1:])
    return exc
