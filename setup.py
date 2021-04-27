#!/usr/bin/env python

from os.path import exists
from setuptools import setup, find_packages

import versioneer

setup(
    name="dask-lithops",
    cmdclass=versioneer.get_cmdclass(),
    version=versioneer.get_version(),
    description="Lithops integration for Dask",
    url="https://github.com/gerardparis/dask-lithops",
    keywords="dask,lithops,distributed",
    license="BSD",
    packages=find_packages(),
    include_package_data=True,
    long_description=(open("README.rst").read() if exists("README.rst") else ""),
    zip_safe=False,
    install_requires=list(open("requirements.txt").read().strip().split("\n")),
    python_requires=">=3.7",
)
