import sys

PY3 = sys.version_info[0] == 3

try:
    reload = reload  # noqa
except NameError:  # pragma: no cover
    from imp import reload  # noqa

try:
    from UserList import UserList  # noqa
except ImportError:  # pragma: no cover
    from collections import UserList  # noqa

try:
    from UserDict import UserDict  # noqa
except ImportError:  # pragma: no cover
    from collections import UserDict  # noqa

import builtins

from io import StringIO

map = map
string = str
string_t = str
long_t = int
text_t = str
range = range
int_types = (int, )

open_fqdn = 'builtins.open'


def items(d):
    return d.items()


def keys(d):
    return d.keys()


def values(d):
    return d.values()


def nextfun(it):
    return it.__next__


exec_ = getattr(builtins, 'exec')


def reraise(tp, value, tb=None):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


class WhateverIO(StringIO):
    def write(self, data):
        if isinstance(data, bytes):
            data = data.encode()
        StringIO.write(self, data)


def with_metaclass(Type, skip_attrs={'__dict__', '__weakref__'}):
    """Class decorator to set metaclass

    Works with both Python 3 and Python 3 and it does not add an extra class in the lookup order like
    ``six.with_metaclass`` does (that is -- it copies the original class instead of using inheritance).

    """

    def _clone_with_metaclass(Class):
        attrs = dict((key, value) for key, value in items(vars(Class)) if key not in skip_attrs)
        return Type(Class.__name__, Class.__bases__, attrs)

    return _clone_with_metaclass

# ############# time.monotonic ################################################

if sys.version_info < (3, 3):

    import platform

    SYSTEM = platform.system()

    if SYSTEM == 'Darwin':
        import ctypes
        from ctypes.util import find_library

        libSystem = ctypes.CDLL('libSystem.dylib')
        CoreServices = ctypes.CDLL(find_library('CoreServices'),
                                   use_errno=True)
        mach_absolute_time = libSystem.mach_absolute_time
        mach_absolute_time.restype = ctypes.c_uint64
        absolute_to_nanoseconds = CoreServices.AbsoluteToNanoseconds
        absolute_to_nanoseconds.restype = ctypes.c_uint64
        absolute_to_nanoseconds.argtypes = [ctypes.c_uint64]

        def _monotonic():
            return absolute_to_nanoseconds(mach_absolute_time()) * 1e-9

    elif SYSTEM == 'Linux':
        # from stackoverflow:
        # questions/1205722/how-do-i-get-monotonic-time-durations-in-python
        import ctypes
        import os

        CLOCK_MONOTONIC = 1  # see <linux/time.h>

        class timespec(ctypes.Structure):
            _fields_ = [
                ('tv_sec', ctypes.c_long),
                ('tv_nsec', ctypes.c_long),
            ]

        librt = ctypes.CDLL('librt.so.1', use_errno=True)
        clock_gettime = librt.clock_gettime
        clock_gettime.argtypes = [
            ctypes.c_int, ctypes.POINTER(timespec),
        ]

        def _monotonic():  # noqa
            t = timespec()
            if clock_gettime(CLOCK_MONOTONIC, ctypes.pointer(t)) != 0:
                errno_ = ctypes.get_errno()
                raise OSError(errno_, os.strerror(errno_))
            return t.tv_sec + t.tv_nsec * 1e-9
    else:
        from time import time as _monotonic
try:
    from time import monotonic
except ImportError:
    monotonic = _monotonic  # noqa
