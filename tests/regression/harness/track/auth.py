"""Authentication fixtures matching the Track regression JWKS."""

from __future__ import annotations

import base64
import time
from typing import Literal

import jwt

JWT_SECRET = "track-regression-test-only-hs256-signing-key"
JWT_KEY_ID = "regression-hs256"

TokenName = Literal[
    "regression-read",
    "regression-write",
    "other-read",
    "other-write",
]

TOKEN_CLAIMS: dict[TokenName, tuple[str, str]] = {
    "regression-read": ("^regression$", "read"),
    "regression-write": ("^regression$", "write"),
    "other-read": ("^other$", "read"),
    "other-write": ("^other$", "write"),
}


def basic(username: str, password: str) -> str:
    encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {encoded}"


def bearer(name: TokenName, *, now: int | None = None) -> str:
    namespace, permission = TOKEN_CLAIMS[name]
    token = jwt.encode(
        {
            "exp": (int(time.time()) if now is None else now) + 3600,
            "namespace": namespace,
            "permission": permission,
        },
        JWT_SECRET,
        algorithm="HS256",
        headers={"kid": JWT_KEY_ID},
    )
    return f"Bearer {token}"
