from twindb_backup.source.file_source import FileSource


def test_suffix():
    fs = FileSource("/foo/bar", "daily")
    assert fs.suffix == "tar"
    fs.suffix += ".gz"
    assert fs.suffix == "tar.gz"
