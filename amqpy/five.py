import sys

# TODO: why do we need this?
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
    monotonic = _monotonic
