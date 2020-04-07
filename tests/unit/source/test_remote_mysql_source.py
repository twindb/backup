import os
from configparser import ConfigParser
from os import path as osp

from io import StringIO
from pathlib import PurePath, Path
from pprint import pformat
from textwrap import dedent

import mock
import py
import pytest

from twindb_backup import INTERVALS, LOG
from twindb_backup.destination.ssh import Ssh
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource
from twindb_backup.ssh.client import SshClient


@mock.patch.object(RemoteMySQLSource, "_save_cfg")
@mock.patch.object(RemoteMySQLSource, "_get_root_my_cnf")
def test__clone_config(mock_get_root, mock_save):
    mock_get_root.return_value = "/etc/my.cnf"
    dst = Ssh("some_remote_dir")
    rmt_sql = RemoteMySQLSource(
        {
            "run_type": INTERVALS[0],
            "backup_type": "full",
            "mysql_connect_info": MySQLConnectInfo("/"),
            "ssh_connection_info": None,
        }
    )
    rmt_sql.clone_config(dst)
    mock_get_root.assert_called_with()
    mock_save.assert_called_with(dst, "/etc/my.cnf")


@pytest.mark.parametrize(
    "mycnfs, expected_result_template",
    [
        (
            {
                "my.cnf": ""
            },
            ["my.cnf"]
        ),
        (
            {
                "my.cnf": dedent(
                    """
                    [mysqld]
                    """
                )
            },
            ["my.cnf"]
        ),
        (
            {
                "my.cnf":
                    dedent(
                        """
                        !includedir conf.d/
                        """
                    ),
                "conf.d/1.cnf": "",
                "conf.d/2.cnf": "",
                "conf.d/3.cnf": "",
            },
            ["my.cnf", "conf.d/1.cnf", "conf.d/2.cnf", "conf.d/3.cnf"]
        ),
        (
            {
                "my.cnf":
                    dedent(
                        """
                        !include conf.d/1.cnf
                        !include conf.d/2.cnf
                        !include conf.d/3.cnf
                        """
                    ),
                "conf.d/1.cnf": "",
                "conf.d/2.cnf": "",
                "conf.d/3.cnf": "",
            },
            ["my.cnf", "conf.d/1.cnf", "conf.d/2.cnf", "conf.d/3.cnf"]
        )
    ],
)
@mock.patch.object(SshClient, "list_files")
@mock.patch.object(SshClient, "get_text_content")
def test___find_all_cnf(mock_get_text_content, mock_list, tmpdir, mycnfs, expected_result_template):
    mycnf_root = Path(tmpdir)

    # Prepare steps (writing config files with content)

    for key in mycnfs.keys():
        mycnf_root.joinpath(key).parent.mkdir(exist_ok=True)
        with open(str(mycnf_root.joinpath(key)), "w") as fp:
            fp.write(mycnfs[key])

    # mock helper functions
    def get_text_content(full_path):
        LOG.debug("Getting content of %s", full_path)
        # cut mysql_root prefix from the full path and lookup for content in the mycnfs dictionary.
        return mycnfs[
            "/".join(
                PurePath(full_path).parts[len(mycnf_root.parts):]
            )
        ]

    def get_list(path, recursive=False, files_only=True):
        return os.listdir(path)

    mock_get_text_content.side_effect = get_text_content
    mock_list.side_effect = get_list
    #
    rmt_sql = RemoteMySQLSource({
        "run_type": INTERVALS[0],
        "backup_type": 'full',
        "mysql_connect_info": MySQLConnectInfo("/"),
        "ssh_connection_info": None
    })
    expected_result = sorted(
        [
            osp.join(str(mycnf_root), item) for item in expected_result_template
        ]
    )
    actual_result = sorted(rmt_sql._find_all_cnf(mycnf_root.joinpath("my.cnf")))
    assert (
        actual_result == expected_result
    ), LOG.error("Expected: %s\nActual: %s" % (pformat(expected_result), pformat(actual_result)))


def test___mem_available():

    mock_client = mock.Mock()
    mock_client.execute.return_value = ("100500", None)

    rmt_sql = RemoteMySQLSource(
        {
            "run_type": INTERVALS[0],
            "backup_type": "full",
            "mysql_connect_info": MySQLConnectInfo("/"),
            "ssh_connection_info": None,
        }
    )
    rmt_sql._ssh_client = mock_client
    assert rmt_sql._mem_available() == 100500 * 1024


def test__mem_available_raise_exception():

    mock_client = mock.Mock()
    mock_client.execute.return_value = ("", None)

    rmt_sql = RemoteMySQLSource(
        {
            "run_type": INTERVALS[0],
            "backup_type": "full",
            "mysql_connect_info": MySQLConnectInfo("/"),
            "ssh_connection_info": None,
        }
    )
    rmt_sql._ssh_client = mock_client
    with pytest.raises(OSError):
        rmt_sql._mem_available()


def test__get_binlog_info_parses_file():
    mock_client = mock.Mock()
    mock_client.execute.return_value = ("mysql-bin.000002\t1054", None)
    rmt_sql = RemoteMySQLSource(
        {
            "run_type": INTERVALS[0],
            "backup_type": "full",
            "mysql_connect_info": MySQLConnectInfo("/"),
            "ssh_connection_info": None,
        }
    )
    rmt_sql._ssh_client = mock_client
    assert rmt_sql._get_binlog_info("foo") == ("mysql-bin.000002", 1054)
