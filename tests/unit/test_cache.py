import os

import pytest

from twindb_backup.cache.cache import Cache, CacheException


def test_init_raises_exception():
    with pytest.raises(CacheException):
        Cache("foo")


def test_in(cache_dir):
    c = Cache(str(cache_dir))

    assert "bar" not in c
    cache_dir.mkdir("foo")
    assert "foo" in c


def test_add(cache_dir, tmpdir):
    c = Cache(str(cache_dir))

    item = str(tmpdir.mkdir("foo"))

    assert item not in c
    c.add(item)
    assert "foo" in c


def test_add_with_key(cache_dir, tmpdir):
    c = Cache(str(cache_dir))

    item = str(tmpdir.mkdir("foo"))

    assert item not in c
    c.add(item, "bar")
    assert "bar" in c


def test_purge(cache_dir):
    c = Cache(str(cache_dir))

    cache_dir.mkdir("foo")
    cache_dir.mkdir("bar")

    assert "foo" in c
    assert "bar" in c

    print(os.listdir(str(cache_dir)))

    c.purge()

    assert "foo" not in c
    assert "bar" not in c
    assert os.path.exists(str(cache_dir))


def test_restore_in(cache_dir, tmpdir):
    c = Cache(str(cache_dir))

    item = tmpdir.mkdir("foo")
    ib = item.join("ibdata1")
    ib.write("content")
    assert os.path.exists(str(ib))

    md = item.mkdir("mysql")
    assert os.path.exists(str(md))

    c.add(str(item))

    dst = str(tmpdir.mkdir("dst"))
    c.restore_in("foo", dst)

    assert os.path.exists(os.path.join(dst, "ibdata1"))
    assert os.path.exists(os.path.join(dst, "mysql"))
