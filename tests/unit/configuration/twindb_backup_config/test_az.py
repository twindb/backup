from textwrap import dedent

from twindb_backup.configuration import TwinDBBackupConfig
from twindb_backup.configuration.destinations.az import AUTH_MODE_CONNECTION_STRING, AUTH_MODE_MANAGED_IDENTITY


def test_az_connection_string(config_file):
    tbc = TwinDBBackupConfig(config_file=str(config_file))

    assert tbc.az.auth_mode == AUTH_MODE_CONNECTION_STRING
    assert (
        tbc.az.connection_string
        == "DefaultEndpointsProtocol=https;AccountName=ACCOUNT_NAME;AccountKey=ACCOUNT_KEY;EndpointSuffix=core.windows.net"
    )
    assert tbc.az.account_url is None
    assert tbc.az.container_name == "twindb-backups"
    assert tbc.az.managed_identity_client_id is None
    assert tbc.az.managed_identity_resource_id is None
    assert tbc.az.create_container_if_missing is True
    assert tbc.az.remote_path == "backups/mysql"
    assert tbc.az.max_concurrency == 1


def test_az_managed_identity(tmpdir):
    cfg_file = tmpdir.join("twindb-backup.cfg")
    with open(str(cfg_file), "w") as fp:
        fp.write(
            dedent(
                """
                [source]
                backup_dirs=/etc
                backup_mysql=no

                [destination]
                backup_destination=az

                [az]
                auth_mode=managed_identity
                account_url="https://account-name.blob.core.windows.net"
                container_name="twindb-backups"
                managed_identity_client_id="test-client-id"
                create_container_if_missing=false
                remote_path="/managed/identity"

                [az.client]
                audience="https://storage.azure.com/"
                connection_timeout=20
                """
            )
        )

    tbc = TwinDBBackupConfig(config_file=str(cfg_file))

    assert tbc.az.auth_mode == AUTH_MODE_MANAGED_IDENTITY
    assert tbc.az.connection_string is None
    assert tbc.az.account_url == "https://account-name.blob.core.windows.net"
    assert tbc.az.container_name == "twindb-backups"
    assert tbc.az.managed_identity_client_id == "test-client-id"
    assert tbc.az.managed_identity_resource_id is None
    assert tbc.az.create_container_if_missing is False
    assert tbc.az.remote_path == "managed/identity"
    assert tbc.az.client_config.audience == "https://storage.azure.com/"


def test_az_managed_identity_resource_id(tmpdir):
    resource_id = (
        "/subscriptions/00000000-0000-0000-0000-000000000000"
        "/resourceGroups/example-rg"
        "/providers/Microsoft.ManagedIdentity/userAssignedIdentities"
        "/example-mi"
    )
    cfg_file = tmpdir.join("twindb-backup.cfg")
    with open(str(cfg_file), "w") as fp:
        fp.write(
            dedent(
                f"""
                [source]
                backup_dirs=/etc
                backup_mysql=no

                [destination]
                backup_destination=az

                [az]
                auth_mode=managed_identity
                account_url="https://account-name.blob.core.windows.net"
                container_name="twindb-backups"
                managed_identity_resource_id="{resource_id}"
                create_container_if_missing=false
                remote_path="/backups/mysql"
                """
            )
        )

    tbc = TwinDBBackupConfig(config_file=str(cfg_file))

    assert tbc.az.auth_mode == AUTH_MODE_MANAGED_IDENTITY
    assert tbc.az.connection_string is None
    assert tbc.az.account_url == "https://account-name.blob.core.windows.net"
    assert tbc.az.container_name == "twindb-backups"
    assert tbc.az.managed_identity_client_id is None
    assert tbc.az.managed_identity_resource_id == resource_id
    assert tbc.az.create_container_if_missing is False
    assert tbc.az.remote_path == "backups/mysql"
