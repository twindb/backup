# -*- coding: utf-8 -*-
"""
Module defines modifier that implements asymmetric encryption with gpg
"""
import os

from twindb_backup.modifiers.base import Modifier, ModifierException


class Gpg(Modifier):
    """Asymmetric encryption"""

    def __init__(self, input_stream, recipient, keyring, secret_keyring=None):
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
            raise ModifierException("Keyring file %s does not exit" % keyring)

        self.secret_keyring = secret_keyring
        self.recipient = recipient
        super(Gpg, self).__init__(input_stream)

    @property
    def _modifier_cmd(self):
        """get compression program cmd"""
        return [
            "gpg",
            "--no-default-keyring",
            "--trust-model",
            "always",
            "--keyring",
            self.keyring,
            "--recipient",
            self.recipient,
            "--encrypt",
            "--yes",
            "--batch",
        ]

    @property
    def _unmodifier_cmd(self):
        """get decompression program cmd"""
        return [
            "gpg",
            "--no-default-keyring",
            "--trust-model",
            "always",
            "--secret-keyring",
            self.secret_keyring,
            "--keyring",
            self.keyring,
            "--recipient",
            self.recipient,
            "--decrypt",
            "--yes",
            "--batch",
        ]
