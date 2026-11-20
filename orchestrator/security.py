"""
HMAC-SHA256 signing and verification for commands and events.

Why this exists:
  The web app and orchestrator are separate processes. Signing proves the body
  was not tampered with and is recent (replay window).

Who calls it:
  api.py verifies incoming commands via verify_signed_request().
  outbox.py signs outbound events.
  tools/fake_web_app.py uses the same helpers for both directions.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from typing import Mapping

SIGNATURE_HEADER = "X-Signature"
TIMESTAMP_HEADER = "X-Timestamp"


class SignatureError(Exception):
    """Raised when a signature or timestamp fails validation."""


def compute_signature(secret: str, timestamp: str, body: bytes) -> str:
    """
    Build the hex HMAC-SHA256 over "{timestamp}.{body}".

    Called by sign_request_headers() and verify_signed_request().
    """
    message = f"{timestamp}.".encode("utf-8") + body
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def sign_request_headers(secret: str, body: bytes, timestamp: int | None = None) -> dict[str, str]:
    """
    Produce headers to attach to an HTTP request with a signed body.

    httpx clients in outbox.py and fake_web_app call this before POST/PUT.
    """
    ts = str(int(time.time()) if timestamp is None else timestamp)
    signature = compute_signature(secret, ts, body)
    return {
        TIMESTAMP_HEADER: ts,
        SIGNATURE_HEADER: signature,
    }


def verify_signed_request(
    secret: str,
    headers: Mapping[str, str],
    body: bytes,
    max_age_secs: int = 300,
) -> None:
    """
    Validate X-Timestamp and X-Signature on an incoming request.

    api.py and fake_web_app call this in middleware or route dependencies.
    Raises SignatureError if anything is wrong (caller maps to HTTP 401).
    """
    # Header names may arrive in different casing from ASGI stacks
    normalized = {k.lower(): v for k, v in headers.items()}
    ts_raw = normalized.get(TIMESTAMP_HEADER.lower())
    sig_raw = normalized.get(SIGNATURE_HEADER.lower())

    if not ts_raw or not sig_raw:
        raise SignatureError(f"missing {TIMESTAMP_HEADER} or {SIGNATURE_HEADER}")

    try:
        ts_int = int(ts_raw)
    except ValueError as exc:
        raise SignatureError("timestamp must be an integer unix time in seconds") from exc

    now = int(time.time())
    if ts_int > now + 60:
        raise SignatureError("timestamp is too far in the future")
    if now - ts_int > max_age_secs:
        raise SignatureError(f"timestamp older than {max_age_secs} seconds")

    expected = compute_signature(secret, str(ts_int), body)
    if not hmac.compare_digest(expected, sig_raw.strip()):
        raise SignatureError("signature mismatch")


def redact_secret_for_log(secret: str) -> str:
    """
    Return a safe placeholder for logging settings at startup.

    main.py uses this instead of printing the real HMAC secret.
    """
    if len(secret) <= 4:
        return "****"
    return f"{secret[:2]}****{secret[-2:]}"