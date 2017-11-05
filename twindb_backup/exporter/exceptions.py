# -*- coding: utf-8 -*-
"""
Module for exporters exceptions
"""


class BaseExporterError(Exception):
    """General exporters error"""
    pass


class DataDogExporterError(BaseExporterError):
    """DataDog exporters error"""
    pass
