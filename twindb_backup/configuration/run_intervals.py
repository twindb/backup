"""Run policy configuration"""
from collections import namedtuple

from twindb_backup import INTERVALS

RunIntervals = namedtuple('RunIntervals', INTERVALS)

RunIntervals.__new__.__defaults__ = (
    (True,) * len(INTERVALS)
)
