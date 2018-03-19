import ConfigParser
import StringIO

import mock
import os
import pytest

from twindb_backup import INTERVALS
from twindb_backup.destination.ssh import Ssh
from twindb_backup.source.mysql_source import MySQLConnectInfo
from twindb_backup.source.remote_mysql_source import RemoteMySQLSource
from twindb_backup.ssh.client import SshClient


@mock.patch.object(RemoteMySQLSource, "_save_cfg")
@mock.patch.object(RemoteMySQLSource, "_get_root_my_cnf")
def test__clone_config(mock_get_root, mock_save):
    mock_get_root.return_value = "/etc/my.cnf"
    dst = Ssh()
    rmt_sql = RemoteMySQLSource({
        "run_type": INTERVALS[0],
        "full_backup": INTERVALS[0],
        "mysql_connect_info": MySQLConnectInfo("/"),
        "ssh_connection_info": None
    })
    rmt_sql.clone_config(dst)
    mock_get_root.assert_called_with()
    mock_save.assert_called_with(dst, "/etc/my.cnf")

@pytest.mark.parametrize("root, files", [
    (
        "my.cnf", {
            "my.cnf": """
"""
        },
    ),

    (
        "my.cnf", {
            "my.cnf": """
[mysqld]
"""
        },
    ),

    (
        "my.cnf", {
            "my.cnf": """
"""
        },
    ),

    (
        "my.cnf", {
            "my.cnf": """

!includedir {tmp_dir}/conf.d/
""",
            "conf.d/1.cnf": """
""",
            "conf.d/2.cnf": """
""",
            "conf.d/3.cnf": """
"""
        },
    ),

    (
        "my.cnf", {
            "my.cnf": """

!includedir {tmp_dir}/conf.d/
""",
            "conf.d/1.cnf": """
""",
            "conf.d/2.cnf": """
""",
            "conf.d/3.cnf": """
[mysqld]
"""
        },
    ),

    (
        "my.cnf", {
            "my.cnf": """

!includedir {tmp_dir}/conf.d/
""",
            "conf.d/1.cnf": """
""",
            "conf.d/2.cnf": """
""",
            "conf.d/3.cnf": """
"""
        },
    ),
])
@mock.patch.object(SshClient, "list_files")
@mock.patch.object(SshClient, "get_text_content")
def test___find_all_cnf(mock_get_text_content, mock_list, tmpdir, root, files):
    root_file = "%s/%s" % (str(tmpdir), root)

    # Prepare steps (writing config files with contents)

    for key in files.keys():
        path = key.split('/')
        if len(path) > 1:
            try:
                tmpdir.mkdir('', path[0])
            except Exception:
                pass
        cfg_file = tmpdir.join(key)
        if '!includedir' in files[key]:
            files[key] = files[key].format(tmp_dir=str(tmpdir))
        cfg_file.write(files[key])
        files["%s/%s" % (str(tmpdir), key)] = files.pop(key)

    # Callback for return ConfiParser from local content

    def get_text_content(value):
        return files[value]

    def get_list(value):
        return os.listdir(value)

    mock_get_text_content.side_effect = get_text_content
    mock_list.side_effect = get_list

    dst = Ssh()
    rmt_sql = RemoteMySQLSource({
        "run_type": INTERVALS[0],
        "full_backup": INTERVALS[0],
        "mysql_connect_info": MySQLConnectInfo("/"),
        "ssh_connection_info": None
    })
    assert sorted(rmt_sql._find_all_cnf(dst, root_file)) == sorted(files.keys())


#@pytest.mark.parametrize("root, files, result", [
#    (
#        "my.cnf", {
#            "my.cnf": """
#"""
#        },
#        "my.cnf"
#    ),
#
#    (
#        "my.cnf", {
#            "my.cnf": """
#[mysqld]
#"""
#        },
#        "my.cnf"
#    ),
#
#    (
#        "my.cnf", {
#            "my.cnf": """
#"""
#        },
#        "my.cnf"
#    ),
#
#    (
#        "my.cnf", {
#            "my.cnf": """
#
#!includedir {tmp_dir}/conf.d/
#""",
#            "conf.d/1.cnf": """
#""",
#            "conf.d/2.cnf": """
#""",
#            "conf.d/3.cnf": """
#[mysqld]
#server_id=0
#"""
#        },
#        "conf.d/3.cnf"
#    ),
#
#    (
#        "my.cnf", {
#            "my.cnf": """
#
#!includedir {tmp_dir}/conf.d/
#""",
#            "conf.d/1.cnf": """
#""",
#            "conf.d/2.cnf": """
#""",
#            "conf.d/3.cnf": """
#[mysqld]
#"""
#        },
#        "conf.d/3.cnf"
#    ),
#
#    (
#        "my.cnf", {
#            "my.cnf": """
#
#!includedir {tmp_dir}/conf.d/
#""",
#            "conf.d/1.cnf": """
#""",
#            "conf.d/2.cnf": """
#""",
#            "conf.d/3.cnf": """
#"""
#        },
#        "my.cnf"
#    ),
#])
#@mock.patch.object(RemoteMySQLSource, "_get_server_id")
#def test__save_cfg(mock_get_config, tmpdir, root, files, result):
#    root_file = "%s/%s" % (str(tmpdir), root)
#    result_file = "%s/%s" % (str(tmpdir), result)
#
#    # Prepare steps (writing config files with contents)
#
#    for key in files.keys():
#        path = key.split('/')
#        if len(path) > 1:
#            try:
#                tmpdir.mkdir('', path[0])
#            except Exception:
#                pass
#        cfg_file = tmpdir.join(key)
#        if '!includedir' in files[key]:
#            files[key] = files[key].format(tmp_dir=str(tmpdir))
#        cfg_file.write(files[key])
#        files["%s/%s" % (str(tmpdir), key)] = files.pop(key)
#
#    # Callback for return ConfiParser from local content
#
#    def get_config(value):
#        return files[value]
#
#    mock_get_config.side_effect = get_config
#
#    dst = Ssh()
#    rmt_sql = RemoteMySQLSource({
#        "run_type": INTERVALS[0],
#        "full_backup": INTERVALS[0],
#        "mysql_connect_info": MySQLConnectInfo("/"),
#        "ssh_connection_info": None
#    })
#    rmt_sql._save_cfg(dst, root_file)


def test___mem_available():

    mock_client = mock.Mock()
    mock_client.execute.return_value = ("100500", None)

    rmt_sql = RemoteMySQLSource({
        "run_type": INTERVALS[0],
        "full_backup": INTERVALS[0],
        "mysql_connect_info": MySQLConnectInfo("/"),
        "ssh_connection_info": None
    })
    rmt_sql._ssh_client = mock_client
    assert rmt_sql._mem_available() == 100500 * 1024


def test__mem_available_raise_exception():

    mock_client = mock.Mock()
    mock_client.execute.return_value = ("", None)

    rmt_sql = RemoteMySQLSource({
        "run_type": INTERVALS[0],
        "full_backup": INTERVALS[0],
        "mysql_connect_info": MySQLConnectInfo("/"),
        "ssh_connection_info": None
    })
    rmt_sql._ssh_client = mock_client
    with pytest.raises(OSError):
        rmt_sql._mem_available()


def test__get_binlog_info_parses_file():
    mock_client = mock.Mock()
    mock_client.execute.return_value = ("mysql-bin.000002\t1054", None)
    rmt_sql = RemoteMySQLSource({
        "run_type": INTERVALS[0],
        "full_backup": INTERVALS[0],
        "mysql_connect_info": MySQLConnectInfo("/"),
        "ssh_connection_info": None
    })
    rmt_sql._ssh_client = mock_client
    assert rmt_sql._get_binlog_info('foo') == ("mysql-bin.000002", 1054)
