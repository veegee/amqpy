"""
This module's purpose is to provide compatibility for Python 3.2 and PyPy3 (which is based on Python
3.2).

The primary method of providing compatibility is by backporting Python 3.3+ features through
monkey-patching. However, this is only done when safe, i.e., when monkey-patching does not change
the behaviour of existing objects.
"""
from __future__ import absolute_import, division, print_function, unicode_literals

__metaclass__ = type

import sys
import time
import six

if six.PY2:
    import __builtin__
elif six.PY3:
    import builtins

if sys.version_info < (3, 3):
    # add TimeoutError exception class
    class TimeoutError(OSError):
        pass


def patch():
    if sys.version_info >= (3, 3):
        return

    if not hasattr(time, 'monotonic'):
        from .support import monotonic
        time.monotonic = monotonic.monotonic

    time.perf_counter = time.clock
    if six.PY2:
        __builtin__.TimeoutError = TimeoutError
    elif six.PY3:
        builtins.TimeoutError = TimeoutError
