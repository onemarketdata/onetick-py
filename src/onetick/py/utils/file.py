import os
import zlib

from pathlib import Path
from typing import Union


class FileBuffer:
    '''
    Class holds the file content with goal to delivery
    it to the execution side in case of remote executions.

    The basic implementation reads file content to a property
    that allows to transfer file content as pickled object
    to the server side since the pickling stores all class property
    values.
    '''

    def __init__(self, path: Union[str, os.PathLike]):
        content = Path(path).read_text()
        self._content = zlib.compress(content.encode('utf-8'), level=9)

    def get(self):
        ''' Returns file content '''
        return zlib.decompress(self._content).decode('utf-8')


def file(path: Union[str, os.PathLike]) -> FileBuffer:
    '''
    Helps to build a file buffer that could be used to
    delivery on the remote site to be processed there.
    For example it could be passed as input to the :class:`CSV <onetick.py.CSV>`

    See Also
    --------
        :class:`CSV <onetick.py.CSV>`
    '''
    return FileBuffer(path)
