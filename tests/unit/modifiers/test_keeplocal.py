from twindb_backup.modifiers.keeplocal import KeepLocal


def test_keeplocal(input_file, tmpdir):
    local_dir = str(tmpdir.join('/foo/bar'))
    with open(str(input_file), 'r') as f:
        m = KeepLocal(f, str(local_dir))
        assert m.local_path == local_dir


def test_keeplocal_saves_file(input_file, tmpdir):
    local_copy = tmpdir.join('foo')

    with open(str(input_file), 'r') as f:
        m = KeepLocal(f, str(local_copy))
        with m.get_stream() as m_f:
            remote_copy = m_f.read().decode("utf-8")
            with open(str(local_copy), 'r') as l:
                assert l.read() == remote_copy
