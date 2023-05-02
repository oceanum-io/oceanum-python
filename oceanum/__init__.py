# -*- coding: utf-8 -*-

"""Top-level package for oceanum."""

__author__ = """Oceanum Developers"""
__email__ = "developers@oceanum.science"
__version__ = "0.7.5"


# Suppress tracebacks in an ipython environment
try:
    import sys

    ipython = get_ipython()

    def exception_handler(exception_type, exception, traceback):
        print("%s: %s" % (exception_type.__name__, exception), file=sys.stderr)

    ipython._showtraceback = exception_handler
except:
    pass
