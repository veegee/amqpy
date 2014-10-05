"""
This module's purpose is to provide compatibility for Python 3.2 and PyPy3 (which is based on Python 3.2).

Python 3.2 support may be removed when PyPy3 becomes Python 3.3+ compatible.
"""

import sys
import time
import builtins

if sys.version_info < (3, 3):
    # add TimeoutError exception class
    class TimeoutError(OSError):
        pass


def patch():
    if sys.version_info >= (3, 3):
        pass

    time.perf_counter = time.clock
    builtins.TimeoutError = TimeoutError
