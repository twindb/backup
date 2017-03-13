from subprocess import PIPE

import mock
import pytest

from twindb_backup.modifiers.base import ModifierException
from twindb_backup.modifiers.gpg import Gpg


def test_gpg_init(input_file, keyring_file):

    recipient = 'a@a.com'

    with open(str(input_file)) as stream:
        gpg = Gpg(stream, recipient, str(keyring_file))
        assert gpg.keyring == str(keyring_file)
        assert gpg.recipient == recipient
        assert gpg.input == stream


def test_gpg_raises_exception_if_no_keyring(input_file, tmpdir):
    keyring_file = tmpdir.join('does_not_exit')

    with open(str(input_file)) as stream:
        with pytest.raises(ModifierException):
            Gpg(stream, 'foo@bar', str(keyring_file))


@mock.patch('twindb_backup.modifiers.gpg.Popen')
def test_get_stream(mock_popen, input_file, keyring_file):

    recipient = 'a@a.com'

    with open(str(input_file)) as stream:
        gpg = Gpg(stream, recipient, str(keyring_file))
        with gpg.get_stream() as s:
            cmd = ['gpg',
                   '--no-default-keyring',
                   '--trust-model', 'always',
                   '--keyring', gpg.keyring,
                   '--recipient', gpg.recipient,
                   '--encrypt',
                   '--yes',
                   '--batch']
            mock_popen.assert_called_once_with(cmd, stdin=gpg.input,
                                               stdout=PIPE,
                                               stderr=PIPE)


@mock.patch.object(Gpg, '_revert_stream')
def test_revert_stream(mock__revert_stream, input_file, keyring_file,
                       secret_keyring_file):

    recipient = 'a@a.com'

    with open(str(input_file)) as stream:
        gpg = Gpg(stream, recipient, str(keyring_file),
                  secret_keyring=str(secret_keyring_file))
        gpg.revert_stream()
        cmd = ['gpg',
               '--no-default-keyring',
               '--trust-model', 'always',
               '--secret-keyring', gpg.secret_keyring,
               '--keyring', gpg.keyring,
               '--recipient', gpg.recipient,
               '--decrypt',
               '--yes',
               '--batch']
        mock__revert_stream.assert_called_once_with(cmd)
