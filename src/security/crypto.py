"""
Field-level encryption using Fernet (symmetric, authenticated).
Use for any column that must be encrypted at rest (e.g. API keys stored in DB).
"""

import logging
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken

from src.security.secrets import get_settings

logger = logging.getLogger(__name__)


def _get_fernet() -> Fernet:
    """Return a Fernet instance using the configured encryption key."""
    key = get_settings().get_encryption_key()
    return Fernet(key)


def encrypt_field(plaintext: str) -> str:
    """
    Encrypt a string field. Returns base64-encoded ciphertext.
    Safe to store in VARCHAR columns.
    """
    if not plaintext:
        return plaintext
    f = _get_fernet()
    return f.encrypt(plaintext.encode()).decode()


def decrypt_field(ciphertext: str) -> Optional[str]:
    """
    Decrypt a field encrypted by encrypt_field().
    Returns None if the token is invalid (tampering detected).
    """
    if not ciphertext:
        return ciphertext
    try:
        f = _get_fernet()
        return f.decrypt(ciphertext.encode()).decode()
    except InvalidToken:
        logger.error("Field decryption failed — invalid token (possible tampering)")
        return None


def hash_api_key(raw_key: str) -> str:
    """
    One-way hash for API key storage.
    Uses PBKDF2-HMAC-SHA256 with the configured salt.
    Never store raw API keys — only the hash.
    """
    import hashlib
    salt = get_settings().get_api_salt().encode()
    dk = hashlib.pbkdf2_hmac("sha256", raw_key.encode(), salt, iterations=260_000)
    return dk.hex()


def generate_api_key() -> tuple[str, str]:
    """
    Generate a new API key.
    Returns (raw_key, hashed_key).
    Store only the hash. Give raw_key to the user once — it cannot be recovered.
    """
    import secrets
    raw = secrets.token_urlsafe(32)
    hashed = hash_api_key(raw)
    return raw, hashed
