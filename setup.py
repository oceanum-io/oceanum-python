#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = [
    "click",
    "aiohttp",
    "fsspec",
    "geojson_pydantic",
    "numpy",
    "pandas",
    "geopandas",
    "xarray",
    "zarr",
    "shapely",
    "orjson",
    "requests",
    "pyarrow",
    "python-snappy",
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
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
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
    version="0.4.3-1",
    zip_safe=False,
)
