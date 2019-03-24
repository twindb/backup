from twindb_backup.modifiers.gzip import Gzip
from twindb_backup.modifiers.pigz import Pigz
from twindb_backup.modifiers.bzip2 import Bzip2
from twindb_backup.modifiers.lbzip2 import Lbzip2
import os
import psutil
import io


text = "example text to compress"
filename = 'compression_text.txt'


def test_compression_init():
    fl = io.FileIO(filename, 'w')
    fl.write(text)
    fl.close()


def test_compression_gzip():
    level = 5
    input_stream = io.FileIO(filename, 'r')
    c = Gzip(input_stream, level)

    assert c.suffix == '.gz'
    assert ' '.join(c.get_compression_cmd()) == 'gzip -{0} -c -'.format(level)
    assert ' '.join(c.get_decompression_cmd()) == 'gunzip -c'

    with Gzip(c.get_stream(), level).revert_stream() as s:
        assert text == s.read()
        input_stream.close()


def test_compression_pigz():
    threads = str(max(psutil.cpu_count() - 1, 1))
    level = 5

    input_stream = io.FileIO(filename, 'r')
    c = Pigz(input_stream, threads, level)

    assert c.suffix == '.gz'
    assert ' '.join(c.get_compression_cmd()) == 'pigz -{0} -p {1} -c -'.format(level, threads)
    assert ' '.join(c.get_decompression_cmd()) == 'pigz -p {0} -d -c'.format(threads)

    with Pigz(c.get_stream(), threads, level).revert_stream() as s:
        assert text == s.read()
        input_stream.close()


def test_compression_bzip():
    level = 5
    input_stream = io.FileIO(filename, 'r')
    c = Bzip2(input_stream, level)

    assert c.suffix == '.bz'
    assert ' '.join(c.get_compression_cmd()) == 'bzip2 -{0} -c -'.format(level)
    assert ' '.join(c.get_decompression_cmd()) == 'bunzip2 -d -c'

    with Bzip2(c.get_stream(), level).revert_stream() as s:
        assert text == s.read()
        input_stream.close()


def test_compression_lbzip2():
    threads = str(max(psutil.cpu_count() - 1, 1))
    level = 5

    input_stream = io.FileIO(filename, 'r')
    c = Lbzip2(input_stream, threads, level)

    assert c.suffix == '.bz'
    assert ' '.join(c.get_compression_cmd()) == 'lbzip2 -{0} -n {1} -c -'.format(level, threads)
    assert ' '.join(c.get_decompression_cmd()) == 'lbzip2 -n {0} -d -c'.format(threads)

    with Lbzip2(c.get_stream(), threads, level).revert_stream() as s:
        assert text == s.read()
        input_stream.close()


def test_compression_deinit():
    os.remove('compression_text.txt')
