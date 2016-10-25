#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_twindb_backup
----------------------------------

Tests for `twindb_backup` module.
"""
import pytest
from twindb_backup.source.file_source import FileSource


@pytest.mark.parametrize('path,name', [
    ('/etc/my.cnf',
     '_etc_my.cnf'),
    ('/var/lib/mysql/',
     '_var_lib_mysql')
])
def test_make_file_name_from_full(path, name):
    assert FileSource._sanitize_filename(path) == name
