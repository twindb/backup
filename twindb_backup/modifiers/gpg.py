# -*- coding: utf-8 -*-
"""
Module defines modifier that implements asymmetric encryption with gpg
"""
import os
from contextlib import contextmanager

from subprocess import Popen, PIPE

from twindb_backup import LOG
from twindb_backup.modifiers.base import Modifier, ModifierException


class Gpg(Modifier):
    """Asymmetric encryption"""
    def __init__(self, input_stream, recipient, keyring,
                 secret_keyring=None):
        """
        Modifier that encrypts the input_stream with gpg.

        :param input_stream: Input stream. Must be file object
        :type input_stream: file
        :param recipient: A string the identifiers a recipient.
        Can be an email for example.
        :type recipient: str
        :param keyring: Path to public keyring. Must exist.
        :type keyring: str
        :param secret_keyring: Path to secret keyring.
        :type keyring: str
        :raise: ModifierException if keyring doesn't exist.
        """
        if os.path.exists(keyring):
            self.keyring = keyring
        else:
            raise ModifierException('Keyring file %s does not exit' % keyring)

        self.secret_keyring = secret_keyring
        self.recipient = recipient
        super(Gpg, self).__init__(input_stream)

    @contextmanager
    def get_stream(self):
        """
        Encrypt the input stream and return it as the output stream

        :return: output stream handle
        :raise: OSError if failed to call the gpg command
        """
        with self.input as input_stream:
            proc = Popen(['gpg', '--no-default-keyring',
                          '--trust-model', 'always',
                          '--keyring', self.keyring,
                          '--recipient', self.recipient,
                          '--encrypt',
                          '--yes',
                          '--batch'],
                         stdin=input_stream,
                         stdout=PIPE,
                         stderr=PIPE)
            yield proc.stdout
            cerr = proc.communicate()
            if proc.returncode:
                LOG.error('gpg exited with non-zero code.')
                LOG.error(cerr)

#    @contextmanager
    def revert_stream(self):
        """
        Decrypt the input stream and return it as the output stream

        :return: output stream handle
        :raise: OSError if failed to call the gpg command
        """
        return self._revert_stream(['gpg', '--no-default-keyring',
                                    '--trust-model', 'always',
                                    '--secret-keyring', self.secret_keyring,
                                    '--keyring', self.keyring,
                                    '--recipient', self.recipient,
                                    '--decrypt',
                                    '--yes',
                                    '--batch'])
