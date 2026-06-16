"""Additional middleware utilities."""

import logging

from fastapi import Request

logger = logging.getLogger(__name__)


async def log_request_body_size(request: Request, call_next):
    """Log unusually large request bodies (potential abuse detection)."""
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > 1_000_000:
        logger.warning(
            "Large request body: %s bytes from %s",
            content_length,
            request.client.host if request.client else "unknown",
        )
    return await call_next(request)
