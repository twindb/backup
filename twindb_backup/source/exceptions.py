# -*- coding: utf-8 -*-
"""
Module for backup source exceptions
"""


class SourceError(Exception):
    """General source error"""
    pass


class MySQLSourceError(SourceError):
    """Exceptions in MySQL source"""


class RemoteMySQLSourceError(MySQLSourceError):
    """Exceptions in remote MySQL source"""
