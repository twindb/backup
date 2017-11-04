# -*- coding: utf-8 -*-
"""
Module defines base exporter class.
"""
from abc import abstractmethod


class ExportCategory(object):
    files = 0
    mysql = 1


class MeasureType(object):
    backup = 0
    restore = 1


class BaseExporter(object):  # pylint: disable=too-few-public-methods
    """
    Base exporter class
    """
    def __init__(self):
        pass

    @abstractmethod
    def export(self, category, measure_type, data):
        """
        Send data to server
        """
