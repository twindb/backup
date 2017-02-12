from twindb_backup.modifiers.keeplocal import KeepLocal


def test_keeplocal(input_file):
    with open(str(input_file), 'r') as f:
        m = KeepLocal(f, '/foo/bar')
        assert m.local_path == '/foo/bar'


def test_keeplocal_saves_file(input_file, tmpdir):
    local_copy = tmpdir.join('foo')

    with open(str(input_file), 'r') as f:
        m = KeepLocal(f, str(local_copy))
        with m.get_stream() as m_f:
            remote_copy = m_f.read()
            with open(str(local_copy), 'r') as l:
                assert l.read() == remote_copy
