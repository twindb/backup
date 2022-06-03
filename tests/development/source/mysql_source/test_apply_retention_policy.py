import mock

from twindb_backup.source.mysql_source import MySQLConnectInfo, MySQLSource


@mock.patch.object(MySQLSource, "get_prefix")
@mock.patch.object(MySQLSource, "_delete_local_files")
@mock.patch("twindb_backup.source.mysql_source.get_files_to_delete")
def test_apply_retention_policy(
    mock_get_files_to_delete, mock_delete_local_files, mock_get_prefix, tmpdir
):
    mock_get_files_to_delete.return_value = []
    mock_get_prefix.return_value = "master.box/hourly"
    my_cnf = tmpdir.join("my.cnf")
    mock_config = mock.Mock()
    src = MySQLSource(
        MySQLConnectInfo(str(my_cnf)), "hourly", "full", dst=mock.Mock()
    )
    mock_dst = mock.Mock()
    mock_dst.remote_path = "/foo/bar"

    # noinspection PyTypeChecker
    src.apply_retention_policy(mock_dst, mock_config, "hourly", mock.Mock())

    mock_delete_local_files.assert_called_once_with("mysql", mock_config)
    mock_dst.list_files.assert_called_once_with(
        "/foo/bar/master.box/hourly/mysql/mysql-"
    )


@mock.patch.object(MySQLSource, "_delete_local_files")
@mock.patch("twindb_backup.source.mysql_source.get_files_to_delete")
def test_apply_retention_policy_remove(
    mock_get_files_to_delete, mock_delete_local_files, tmpdir
):

    mock_get_files_to_delete.return_value = ["key-foo"]
    my_cnf = tmpdir.join("my.cnf")
    mock_config = mock.Mock()
    src = MySQLSource(
        MySQLConnectInfo(str(my_cnf)), "hourly", "full", dst=mock.Mock()
    )
    mock_dst = mock.Mock()
    mock_dst.remote_path = "/foo/bar"
    mock_dst.basename.return_value = "key-foo"

    mock_status = mock.Mock()

    # noinspection PyTypeChecker
    src.apply_retention_policy(mock_dst, mock_config, "hourly", mock_status)

    mock_status.remove.assert_called_once_with("key-foo")
