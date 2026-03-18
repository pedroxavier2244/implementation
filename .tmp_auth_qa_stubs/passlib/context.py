import hashlib
import hmac
import os


class CryptContext:
    def __init__(self, schemes=None, deprecated=None):
        self.schemes = schemes or []
        self.deprecated = deprecated

    def hash(self, plain: str) -> str:
        salt = os.urandom(16).hex()
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            plain.encode("utf-8"),
            salt.encode("utf-8"),
            120000,
        ).hex()
        return f"stub$sha256${salt}${digest}"

    def verify(self, plain: str, hashed: str) -> bool:
        try:
            _, _, salt, expected = hashed.split("$", 3)
        except ValueError:
            return False
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            plain.encode("utf-8"),
            salt.encode("utf-8"),
            120000,
        ).hex()
        return hmac.compare_digest(digest, expected)
