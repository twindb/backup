from textwrap import dedent

import mock
import pytest

from twindb_backup.configuration import TwinDBBackupConfig
from twindb_backup.modifiers import Bzip2, Gzip, Lbzip2, Pigz


def test_default(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))
    assert tbc.compression is not None
    assert tbc.compression.program == "gzip"


@pytest.mark.parametrize(
    "content, compressor, expected_kw",
    [
        (
            dedent(
                """
            """
            ),
            Gzip,
            {},
        ),
        (
            dedent(
                """
            [compression]
            program=gzip
            level = 3
            """
            ),
            Gzip,
            {"level": 3},
        ),
        (
            dedent(
                """
            [compression]
            program=bzip2
            """
            ),
            Bzip2,
            {},
        ),
        (
            dedent(
                """
            [compression]
            program=bzip2
            level: 5
            """
            ),
            Bzip2,
            {"level": 5},
        ),
        (
            dedent(
                """
            [compression]
            program=lbzip2
            level: 6
            """
            ),
            Lbzip2,
            {"level": 6},
        ),
        (
            dedent(
                """
            [compression]
            program=lbzip2
            level: 6
            threads: 128
            """
            ),
            Lbzip2,
            {"level": 6, "threads": 128},
        ),
        (
            dedent(
                """
            [compression]
            program=pigz
            level: 7
            threads: 129
            """
            ),
            Pigz,
            {"level": 7, "threads": 129},
        ),
    ],
)
def test_get_modifier(content, compressor, expected_kw, tmpdir):
    cfg_file = tmpdir.join("twindb-backup.cfg")
    with open(str(cfg_file), "w") as fp:
        fp.write(content)

    tbc = TwinDBBackupConfig(config_file=str(cfg_file))

    with mock.patch.object(compressor, "__init__") as mock_compr:
        mock_compr.return_value = None
        mock_stream = mock.Mock()
        assert isinstance(tbc.compression.get_modifier(mock_stream), compressor)
        mock_compr.assert_called_once_with(mock_stream, **expected_kw)
