# -*- coding: utf-8 -*-

"""Top-level package for oceanum."""

__author__ = """Oceanum Developers"""
__email__ = "developers@oceanum.science"
__version__ = "0.11.7"


# Suppress tracebacks in an ipython environment
try:
    import sys

    ipython = get_ipython()
    _default_handler = ipython._showtraceback

    def exception_handler(exception_type, exception, traceback):
        if hasattr(exception_type, "oceanum_exc"):
            print("%s: %s" % (exception_type.__name__, exception), file=sys.stderr)
        else:
            _default_handler(exception_type, exception, traceback)

    ipython._showtraceback = exception_handler
except:
    pass
