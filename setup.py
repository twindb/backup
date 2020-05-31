#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from setuptools import setup, find_packages

del os.link

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

with open("requirements.txt") as f:
    requirements = f.read().strip().split("\n")

with open("requirements_dev.txt") as f:
    test_requirements = f.read().strip().split("\n")

setup(
    name="twindb-backup",
    version="2.20.2",
    description="TwinDB Backup tool for files, MySQL et al.",
    long_description=readme + "\n\n" + history,
    author="TwinDB Development Team",
    author_email="dev@twindb.com",
    url="https://github.com/twindb/twindb_backup",
    packages=find_packages(exclude=("tests*",)),
    package_dir={"twindb_backup": "twindb_backup"},
    entry_points={"console_scripts": ["twindb-backup=twindb_backup.cli:main"]},
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords="twindb_backup",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
    ],
    test_suite="tests",
    tests_require=test_requirements,
)
