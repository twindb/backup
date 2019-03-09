"""Retention policy configuration"""
from collections import namedtuple

RetentionPolicy = namedtuple(
    'RetentionPolicy',
    [
        'hourly',
        'daily',
        'weekly',
        'monthly',
        'yearly'
    ]
)

RetentionPolicy.__new__.__defaults__ = (
    24, 7, 4, 12, 3
)
