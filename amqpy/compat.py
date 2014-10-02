"""
This module's purpose is to provide compatibility for Python 3.2 and PyPy3 (which is based on Python 3.2).
"""

import sys

if sys.version_info < (3, 3):
    TimeoutError = Exception
else:
    TimeoutError = TimeoutError
