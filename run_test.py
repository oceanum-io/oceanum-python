#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Simple script to run the test_verify_parameter_in_zarr_client test"""
import os
import sys
import pytest

if __name__ == "__main__":
    # Run the specific test
    exit_code = pytest.main(["-v", "tests/test_verify_parameter.py::test_verify_parameter_in_zarr_client"])
    sys.exit(exit_code)
