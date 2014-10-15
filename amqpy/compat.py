"""
This module's purpose is to provide compatibility for Python 3.2 and PyPy3 (which is based on Python
3.2).

The primary method of providing compatibility is by backporting Python 3.3+ features through
monkey-patching. However, this is only done when safe, i.e., when monkey-patching does not change
the behaviour of existing objects.
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
        return

    time.perf_counter = time.clock
    builtins.TimeoutError = TimeoutError
