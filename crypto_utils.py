from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import os

def encrypt_message(message_bytes, key):
    # Generar un IV (Initialization Vector) aleatorio de 16 bytes
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(key), modes.CFB(iv))
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(message_bytes) + encryptor.finalize()
    # Retornar IV + ciphertext
    return iv + ciphertext

def decrypt_message(encrypted_bytes, key):
    # Extraer el IV (primeros 16 bytes)
    iv = encrypted_bytes[:16]
    ciphertext = encrypted_bytes[16:]
    cipher = Cipher(algorithms.AES(key), modes.CFB(iv))
    decryptor = cipher.decryptor()
    message = decryptor.update(ciphertext) + decryptor.finalize()
    return message
