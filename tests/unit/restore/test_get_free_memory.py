from twindb_backup.restore import get_free_memory


def test_get_free_memory():
    mem = get_free_memory()
    assert isinstance(mem, int)
    assert mem > 0
