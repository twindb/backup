from twindb_backup.configuration import GPGConfig


def test_init():
    gpg = GPGConfig(
        recipient="bob",
        keyring="/dev/null"
    )
    assert gpg.recipient == "bob"
    assert gpg.keyring == "/dev/null"
