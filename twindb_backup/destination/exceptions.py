# -*- coding: utf-8 -*-
"""
Module for destination exceptions
"""


class DestinationError(Exception):
    """General destination error"""
    pass


class S3DestinationError(DestinationError):
    """S3 destination errors"""
    pass


class GCSDestinationError(DestinationError):
    """GCS destination errors"""
    pass


class SshDestinationError(DestinationError):
    """SSH destination errors"""
    pass
