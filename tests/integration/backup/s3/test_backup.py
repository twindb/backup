import json
import os
from io import StringIO

import magic

from tests.integration.conftest import (
    assert_and_pause,
    docker_execute,
    get_twindb_config_dir,
    pause_test,
)
from twindb_backup import LOG
from twindb_backup.destination.s3 import S3


def test__take_file_backup(
    master1, docker_client, s3_client, config_content_files_only
):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1["Id"])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = "/etc/twindb/twindb-backup-1.cfg"

    backup_dir = "/etc/twindb"

    with open(twindb_config_host, "w") as fp:
        content = config_content_files_only.format(
            TEST_DIR=backup_dir,
            AWS_ACCESS_KEY_ID=os.environ["AWS_ACCESS_KEY_ID"],
            AWS_SECRET_ACCESS_KEY=os.environ["AWS_SECRET_ACCESS_KEY"],
            BUCKET=s3_client.bucket,
        )
        fp.write(content)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "hourly",
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    # Check that backup copy is in "twindb-backup ls" output
    hostname = "master1_1"
    s3_backup_path = "s3://%s/%s/hourly/files/%s" % (
        s3_client.bucket,
        hostname,
        backup_dir.replace("/", "_"),
    )
    cmd = ["twindb-backup", "--debug", "--config", twindb_config_guest, "ls"]

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)

    assert_and_pause((ret == 0,), cout)
    assert_and_pause(
        (s3_backup_path in cout,), "%s is not in %s" % (s3_backup_path, cout)
    )

    backup_to_restore = None
    for line in StringIO(cout):
        if line.startswith(s3_backup_path):
            backup_to_restore = line.strip()
            break

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "restore",
        "file",
        "--dst",
        "/tmp/restore",
        backup_to_restore,
    ]
    assert_and_pause((backup_to_restore is not None,), s3_backup_path)

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    # Check that restored file exists
    path_to_file_restored = "/tmp/restore/etc/twindb/twindb-backup-1.cfg"
    cmd = ["ls", path_to_file_restored]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    # And content is same
    cmd = [
        "diff",
        "/tmp/restore/etc/twindb/twindb-backup-1.cfg",
        twindb_config_guest,
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    # empty output
    assert_and_pause((not cout,), cout)
    # zero exit code if no differences
    assert_and_pause((ret == 0,), "%s exited with %d" % (" ".join(cmd), ret))


def test__take_mysql_backup(
    master1, docker_client, s3_client, config_content_mysql_only, client_my_cnf
):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1["Id"])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = "/etc/twindb/twindb-backup-1.cfg"
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(client_my_cnf)

    with open(twindb_config_host, "w") as fp:
        content = config_content_mysql_only.format(
            AWS_ACCESS_KEY_ID=os.environ["AWS_ACCESS_KEY_ID"],
            AWS_SECRET_ACCESS_KEY=os.environ["AWS_SECRET_ACCESS_KEY"],
            BUCKET=s3_client.bucket,
            daily_copies=1,
            hourly_copies=2,
            MY_CNF="/etc/twindb/my.cnf",
        )
        fp.write(content)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "hourly",
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    cmd = ["twindb-backup", "--config", twindb_config_guest, "status"]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    LOG.debug("STDOUT: %s", cout)
    key = list(json.loads(cout)["hourly"].keys())[0]

    assert_and_pause((key.endswith(".xbstream.gz"),), key)


def test__take_mysql_backup_retention(
    master1, docker_client, s3_client, config_content_mysql_only, client_my_cnf
):

    twindb_config_dir = get_twindb_config_dir(docker_client, master1["Id"])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = "/etc/twindb/twindb-backup-1.cfg"
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(client_my_cnf)

    with open(twindb_config_host, "w") as fp:
        content = config_content_mysql_only.format(
            AWS_ACCESS_KEY_ID=os.environ["AWS_ACCESS_KEY_ID"],
            AWS_SECRET_ACCESS_KEY=os.environ["AWS_SECRET_ACCESS_KEY"],
            BUCKET=s3_client.bucket,
            daily_copies=1,
            hourly_copies=2,
            MY_CNF="/etc/twindb/my.cnf",
        )
        fp.write(content)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "daily",
    ]

    for i in range(0, 3):
        ret, cout = docker_execute(docker_client, master1["Id"], cmd)
        print(cout)
        assert ret == 0

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "hourly",
    ]
    for i in range(0, 3):
        ret, cout = docker_execute(docker_client, master1["Id"], cmd)
        print(cout)
        assert ret == 0

    cmd = ["twindb-backup", "--config", twindb_config_guest, "status"]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    assert_and_pause((ret == 0,), cout)

    status = json.loads(cout)

    assert_and_pause((len(status["daily"].keys()) == 1,), status)
    assert_and_pause((len(status["hourly"].keys()) == 2,), status)


def test__s3_find_files_returns_sorted(
    master1, docker_client, s3_client, config_content_mysql_only, client_my_cnf
):
    # cleanup the bucket first
    s3_client.delete_all_objects()

    twindb_config_dir = get_twindb_config_dir(docker_client, master1["Id"])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = "/etc/twindb/twindb-backup-1.cfg"
    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(client_my_cnf)

    with open(twindb_config_host, "w") as fp:
        content = config_content_mysql_only.format(
            AWS_ACCESS_KEY_ID=os.environ["AWS_ACCESS_KEY_ID"],
            AWS_SECRET_ACCESS_KEY=os.environ["AWS_SECRET_ACCESS_KEY"],
            BUCKET=s3_client.bucket,
            daily_copies=5,
            hourly_copies=2,
            MY_CNF="/etc/twindb/my.cnf",
        )
        fp.write(content)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "daily",
    ]

    n_runs = 3
    for x in range(n_runs):
        ret, cout = docker_execute(docker_client, master1["Id"], cmd)
        print(cout)
        assert ret == 0
    hostname = "master1_1"
    dst = S3(
        bucket=s3_client.bucket,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    for x in range(10):
        result = dst.list_files(dst.remote_path, pattern="/daily/")
        assert len(result) == n_runs
        assert result == sorted(result)
        prefix = "{remote_path}/{hostname}/{run_type}/mysql/mysql-".format(
            remote_path=dst.remote_path, hostname=hostname, run_type="daily"
        )
        files = dst.list_files(prefix)
        assert len(files) == n_runs
        assert files == sorted(files)


def test_take_file_backup_with_aenc(
    master1,
    docker_client,
    s3_client,
    config_content_files_aenc,
    gpg_public_key,
    gpg_private_key,
):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1["Id"])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = "/etc/twindb/twindb-backup-1.cfg"
    backup_dir = "/etc/twindb"

    gpg_public_key_path_host = "%s/public_key" % twindb_config_dir
    gpg_private_key_path_host = "%s/private_key" % twindb_config_dir
    gpg_private_key_path_guest = "/etc/twindb/private_key"

    gpg_keyring = "/etc/twindb/keyring"
    gpg_secret_keyring = "/etc/twindb/secret_keyring"

    with open(gpg_public_key_path_host, "w") as fd:
        fd.write(gpg_public_key)
    with open(gpg_private_key_path_host, "w") as fd:
        fd.write(gpg_private_key)

    cmd = ["rm", "-f", gpg_keyring, gpg_secret_keyring]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0

    cmd = [
        "gpg",
        "--no-default-keyring",
        "--keyring",
        gpg_keyring,
        "--secret-keyring",
        gpg_secret_keyring,
        "--yes",
        "--no-tty",
        "--batch",
        "--import",
        gpg_private_key_path_guest,
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0

    with open(twindb_config_host, "w") as fp:
        content = config_content_files_aenc.format(
            TEST_DIR=backup_dir,
            AWS_ACCESS_KEY_ID=os.environ["AWS_ACCESS_KEY_ID"],
            AWS_SECRET_ACCESS_KEY=os.environ["AWS_SECRET_ACCESS_KEY"],
            BUCKET=s3_client.bucket,
            gpg_keyring=gpg_keyring,
            gpg_secret_keyring=gpg_secret_keyring,
        )
        fp.write(content)

    # write some content to the directory
    with open(os.path.join(twindb_config_dir, "file"), "w") as f:
        f.write("Hello world.")

    hostname = "master1_1"
    s3_backup_path = "s3://%s/%s/hourly/files/%s" % (
        s3_client.bucket,
        hostname,
        backup_dir.replace("/", "_"),
    )

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "hourly",
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0

    cmd = ["twindb-backup", "--debug", "--config", twindb_config_guest, "ls"]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0
    assert s3_backup_path in cout

    backup_to_restore = None
    for line in StringIO(cout):
        if line.startswith(s3_backup_path):
            backup_to_restore = line.strip()
            break
    assert backup_to_restore.endswith(".tar.gz.gpg")
    key = backup_to_restore.lstrip("s3://").lstrip(s3_client.bucket).lstrip("/")
    local_copy = "%s/backup_to_restore.tar.gz.gpg" % twindb_config_dir
    s3_client.s3_client.download_file(s3_client.bucket, key, local_copy)
    assert magic.from_file(local_copy) == "data" or magic.from_file(
        local_copy
    ).startswith("PGP RSA encrypted")

    dest_dir = "/tmp/simple_backup_aenc"
    cmd = ["mkdir", "-p", "/tmp/simple_backup_aenc"]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "restore",
        "file",
        "--dst",
        dest_dir,
        backup_to_restore,
    ]

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0
    path_to_file_restored = "%s%s/file" % (dest_dir, backup_dir)
    cmd = ["test", "-f", path_to_file_restored]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0

    # And content is same
    path_to_file_orig = "%s/file" % backup_dir
    cmd = ["diff", "-Nur", path_to_file_orig, path_to_file_restored]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0
    assert not cout


def test__take_mysql_backup_aenc_suffix_gpg(
    master1,
    docker_client,
    s3_client,
    config_content_mysql_aenc,
    gpg_public_key,
    gpg_private_key,
    client_my_cnf,
):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1["Id"])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = "/etc/twindb/twindb-backup-1.cfg"

    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(client_my_cnf)

    gpg_public_key_path_host = "%s/public_key" % twindb_config_dir
    gpg_private_key_path_host = "%s/private_key" % twindb_config_dir
    gpg_private_key_path_guest = "/etc/twindb/private_key"

    gpg_keyring = "/etc/twindb/keyring"
    gpg_secret_keyring = "/etc/twindb/secret_keyring"

    with open(gpg_public_key_path_host, "w") as fd:
        fd.write(gpg_public_key)
    with open(gpg_private_key_path_host, "w") as fd:
        fd.write(gpg_private_key)

    cmd = ["rm", "-f", gpg_keyring, gpg_secret_keyring]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0

    cmd = [
        "gpg",
        "--no-default-keyring",
        "--keyring",
        gpg_keyring,
        "--secret-keyring",
        gpg_secret_keyring,
        "--yes",
        "--no-tty",
        "--batch",
        "--import",
        gpg_private_key_path_guest,
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0

    with open(twindb_config_host, "w") as fp:
        content = config_content_mysql_aenc.format(
            AWS_ACCESS_KEY_ID=os.environ["AWS_ACCESS_KEY_ID"],
            AWS_SECRET_ACCESS_KEY=os.environ["AWS_SECRET_ACCESS_KEY"],
            BUCKET=s3_client.bucket,
            gpg_keyring=gpg_keyring,
            gpg_secret_keyring=gpg_secret_keyring,
            daily_copies=1,
            hourly_copies=2,
            MY_CNF="/etc/twindb/my.cnf",
        )
        fp.write(content)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "daily",
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0
    cmd = ["twindb-backup", "--config", twindb_config_guest, "status"]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert ret == 0
    key = list(json.loads(cout)["daily"].keys())[0]
    assert key.endswith("xbstream.gz.gpg")
    local_copy = "%s/mysql_backup.tar.gz.gpg" % twindb_config_dir

    s3_client.s3_client.download_file(s3_client.bucket, key, local_copy)
    assert magic.from_file(local_copy) == "data" or magic.from_file(
        local_copy
    ).startswith("PGP RSA encrypted")


def test_take_mysql_backup_aenc_restores_full(
    master1,
    docker_client,
    s3_client,
    config_content_mysql_aenc,
    gpg_public_key,
    gpg_private_key,
    tmpdir,
    client_my_cnf,
):

    twindb_config_dir = get_twindb_config_dir(docker_client, master1["Id"])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = "/etc/twindb/twindb-backup-1.cfg"

    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(client_my_cnf)

    gpg_public_key_path_host = "%s/public_key" % twindb_config_dir
    gpg_private_key_path_host = "%s/private_key" % twindb_config_dir
    gpg_private_key_path_guest = "/etc/twindb/private_key"

    gpg_keyring = "/etc/twindb/keyring"
    gpg_secret_keyring = "/etc/twindb/secret_keyring"

    with open(gpg_public_key_path_host, "w") as fd:
        fd.write(gpg_public_key)
    with open(gpg_private_key_path_host, "w") as fd:
        fd.write(gpg_private_key)

    cmd = ["rm", "-f", gpg_keyring, gpg_secret_keyring]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    cmd = [
        "gpg",
        "--no-default-keyring",
        "--keyring",
        gpg_keyring,
        "--secret-keyring",
        gpg_secret_keyring,
        "--yes",
        "--no-tty",
        "--batch",
        "--import",
        gpg_private_key_path_guest,
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    with open(twindb_config_host, "w") as fp:
        content = config_content_mysql_aenc.format(
            AWS_ACCESS_KEY_ID=os.environ["AWS_ACCESS_KEY_ID"],
            AWS_SECRET_ACCESS_KEY=os.environ["AWS_SECRET_ACCESS_KEY"],
            BUCKET=s3_client.bucket,
            gpg_keyring=gpg_keyring,
            gpg_secret_keyring=gpg_secret_keyring,
            daily_copies=1,
            hourly_copies=2,
            MY_CNF="/etc/twindb/my.cnf",
        )
        fp.write(content)
    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "daily",
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    cmd = ["twindb-backup", "--config", twindb_config_guest, "status"]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    key = list(json.loads(cout)["daily"].keys())[0]

    backup_copy = "s3://" + s3_client.bucket + "/" + key
    dst_dir = str(tmpdir.mkdir("dst"))
    ret, cout = docker_execute(
        docker_client, master1["Id"], ["mkdir", "-p", str(dst_dir)]
    )
    print(cout)
    assert_and_pause((ret == 0,), cout)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        str(twindb_config_guest),
        "restore",
        "mysql",
        backup_copy,
        "--dst",
        dst_dir,
    ]

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    print("Files in restored datadir:")
    ret, cout = docker_execute(docker_client, master1["Id"], ["find", dst_dir])
    print(cout)
    assert_and_pause((ret == 0,), cout)

    files_to_test = []
    mysql_files = [
        "ibdata1",
        "ib_logfile0",
        "ib_logfile1",
        "backup-my.cnf",
        "xtrabackup_logfile",
    ]
    for datadir_file in mysql_files:
        files_to_test += ["test -f %s/%s" % (dst_dir, datadir_file)]
    cmd = ["bash", "-c", " && ".join(files_to_test)]

    print(cmd)
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    cmd = [
        "bash",
        "-c",
        "test -f {datadir}/_config/etc/my.cnf "
        "|| test -f {datadir}/_config/etc/mysql/my.cnf".format(datadir=dst_dir),
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)


def test_take_mysql_backup_aenc_restores_inc(
    master1,
    docker_client,
    s3_client,
    config_content_mysql_aenc,
    gpg_public_key,
    gpg_private_key,
    tmpdir,
    client_my_cnf,
):
    twindb_config_dir = get_twindb_config_dir(docker_client, master1["Id"])

    twindb_config_host = "%s/twindb-backup-1.cfg" % twindb_config_dir
    twindb_config_guest = "/etc/twindb/twindb-backup-1.cfg"

    my_cnf_path = "%s/my.cnf" % twindb_config_dir

    with open(my_cnf_path, "w") as my_cnf:
        my_cnf.write(client_my_cnf)

    gpg_public_key_path_host = "%s/public_key" % twindb_config_dir
    gpg_private_key_path_host = "%s/private_key" % twindb_config_dir
    gpg_private_key_path_guest = "/etc/twindb/private_key"

    gpg_keyring = "/etc/twindb/keyring"
    gpg_secret_keyring = "/etc/twindb/secret_keyring"

    with open(gpg_public_key_path_host, "w") as fd:
        fd.write(gpg_public_key)
    with open(gpg_private_key_path_host, "w") as fd:
        fd.write(gpg_private_key)

    cmd = ["rm", "-f", gpg_keyring, gpg_secret_keyring]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    cmd = [
        "gpg",
        "--no-default-keyring",
        "--keyring",
        gpg_keyring,
        "--secret-keyring",
        gpg_secret_keyring,
        "--yes",
        "--no-tty",
        "--batch",
        "--import",
        gpg_private_key_path_guest,
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    with open(twindb_config_host, "w") as fp:
        content = config_content_mysql_aenc.format(
            AWS_ACCESS_KEY_ID=os.environ["AWS_ACCESS_KEY_ID"],
            AWS_SECRET_ACCESS_KEY=os.environ["AWS_SECRET_ACCESS_KEY"],
            BUCKET=s3_client.bucket,
            gpg_keyring=gpg_keyring,
            gpg_secret_keyring=gpg_secret_keyring,
            daily_copies=1,
            hourly_copies=2,
            MY_CNF="/etc/twindb/my.cnf",
        )
        fp.write(content)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "daily",
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        twindb_config_guest,
        "backup",
        "hourly",
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    cmd = ["twindb-backup", "--config", twindb_config_guest, "status"]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    key = list(json.loads(cout)["hourly"].keys())[0]

    backup_copy = "s3://" + s3_client.bucket + "/" + key
    dst_dir = str(tmpdir.mkdir("dst"))
    ret, cout = docker_execute(
        docker_client, master1["Id"], ["mkdir", "-p", str(dst_dir)]
    )
    print(cout)
    assert_and_pause((ret == 0,), cout)

    cmd = [
        "twindb-backup",
        "--debug",
        "--config",
        str(twindb_config_guest),
        "restore",
        "mysql",
        backup_copy,
        "--dst",
        dst_dir,
    ]

    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    print("Files in restored datadir:")
    ret, cout = docker_execute(docker_client, master1["Id"], ["find", dst_dir])
    print(cout)
    assert_and_pause((ret == 0,), cout)

    files_to_test = []
    for datadir_file in [
        "ibdata1",
        "ib_logfile0",
        "ib_logfile1",
        "backup-my.cnf",
        "xtrabackup_logfile",
    ]:
        files_to_test += ["test -f %s/%s" % (dst_dir, datadir_file)]
    cmd = ["bash", "-c", " && ".join(files_to_test)]

    print(cmd)
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)

    cmd = [
        "bash",
        "-c",
        "test -f {datadir}/_config/etc/my.cnf "
        "|| test -f {datadir}/_config/etc/mysql/my.cnf".format(datadir=dst_dir),
    ]
    ret, cout = docker_execute(docker_client, master1["Id"], cmd)
    print(cout)
    assert_and_pause((ret == 0,), cout)
