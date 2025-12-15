import time


INDENTS: list[int] = []


def debug(f):
    def wrapper(*args, **kwargs):
        start = time.time()
        cur_indent = INDENTS[-1] + 1 if INDENTS else 0
        INDENTS.append(cur_indent)
        print(' ' * 4 * cur_indent + f.__name__, 'started', args, kwargs)
        result = f(*args, **kwargs)
        print(' ' * 4 * cur_indent + f.__name__, 'executed in', time.time() - start)
        INDENTS.pop()
        return result
    return wrapper
