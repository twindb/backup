"""Module that describes exceptions of twindb_backup module"""


class OperationError(Exception):
    """High level exceptions of twindb_backup package"""
    pass


class LockWaitTimeoutError(Exception):
    """Class that describes exception of lock wait timeout"""
    pass
