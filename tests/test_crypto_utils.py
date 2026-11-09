import os

import pytest
from cryptography.exceptions import InvalidTag

from crypto_utils import decrypt_message, encrypt_message


@pytest.mark.parametrize("message", [b"", b"EasyCab secure message", bytes(range(256))])
def test_round_trip(message):
    key = os.urandom(32)
    ciphertext = encrypt_message(message, key)
    assert ciphertext != message
    assert decrypt_message(ciphertext, key) == message
    assert encrypt_message(message, key) != ciphertext


def test_tampering_is_rejected():
    key = os.urandom(32)
    ciphertext = bytearray(encrypt_message(b"trip status", key))
    ciphertext[-1] ^= 1
    with pytest.raises(InvalidTag):
        decrypt_message(bytes(ciphertext), key)


def test_wrong_key_is_rejected():
    ciphertext = encrypt_message(b"trip status", os.urandom(32))
    with pytest.raises(InvalidTag):
        decrypt_message(ciphertext, os.urandom(32))


def test_short_payload_is_rejected():
    with pytest.raises(ValueError):
        decrypt_message(b"too short", os.urandom(32))
