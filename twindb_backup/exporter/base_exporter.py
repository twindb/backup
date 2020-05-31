# -*- coding: utf-8 -*-
"""
Module defines base exporter class.
"""
from abc import abstractmethod


class ExportCategory(object):  # pylint: disable=too-few-public-methods
    """Category of export data: files or mysql"""

    files = 0
    mysql = 1


class ExportMeasureType(object):  # pylint: disable=too-few-public-methods
    """Type of measure time: backup or restore"""

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
