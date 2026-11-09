import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

NONCE_SIZE = 12


def encrypt_message(message_bytes, key):
    nonce = os.urandom(NONCE_SIZE)
    ciphertext = AESGCM(key).encrypt(nonce, message_bytes, None)
    return nonce + ciphertext


def decrypt_message(encrypted_bytes, key):
    if len(encrypted_bytes) < NONCE_SIZE + 16:
        raise ValueError("El mensaje cifrado no tiene un formato válido.")

    nonce = encrypted_bytes[:NONCE_SIZE]
    ciphertext = encrypted_bytes[NONCE_SIZE:]
    return AESGCM(key).decrypt(nonce, ciphertext, None)