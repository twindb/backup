"""
Modifiers module.

Modifier take a stream as input, do something with it (compress, encrypt, etc)
and return the modified stream for a next modifier or backup destination.

Modifiers also do reverse operation - i.e. decompress, decrypt.
"""
from twindb_backup.modifiers.bzip2 import Bzip2
from twindb_backup.modifiers.gzip import Gzip
from twindb_backup.modifiers.lbzip2 import Lbzip2
from twindb_backup.modifiers.pigz import Pigz

COMPRESSION_MODIFIERS = {
    'gzip': {
        'class': Gzip,
        'kwargs': ['level']
    },
    'bzip2': {
        'class': Bzip2,
        'kwargs': ['level']
    },
    'lbzip2': {
        'class': Lbzip2,
        'kwargs': ['threads', 'level']
    },
    'pigz': {
        'class': Pigz,
        'kwargs': ['threads', 'level']
    },
}
