# -*- coding: utf-8 -*-
"""
Module defines base exporter class.
"""
from abc import abstractmethod


class BaseExporter(object):  # pylint: disable=too-few-public-methods
    """
    Base exporter class
    """
    def __init__(self):
        pass

    @abstractmethod
    def export(self, data):
        """
        Send data to server
        """
