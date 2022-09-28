#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

import oceanum

requirements = [
    "click",
    "aiohttp",
    "fsspec",
    "numpy",
    "pandas",
    "geopandas",
    "xarray",
    "zarr",
    "h5netcdf",
    "shapely",
    "orjson",
    "requests",
    "pyarrow",
    "python-snappy",
    "geojson-pydantic",
]

setup_requirements = [
    "pytest-runner",
]

test_requirements = [
    "pytest",
]

setup(
    author="Oceanum Developers",
    author_email="developers@oceanum.science",
    description="Library for oceanum.io services",
    entry_points={
        "console_scripts": [
            "oceanum=oceanum.cli:main",
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="oceanum",
    documentation="https://oceanum-python.readthedocs.io",
    name="oceanum",
    packages=find_packages(),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/oceanum/oceanum-python",
    version=oceanum.__version__,
    zip_safe=False,
)
