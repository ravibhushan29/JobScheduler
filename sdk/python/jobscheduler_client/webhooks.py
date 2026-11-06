"""Verify webhooks sent by the Job Scheduler (no dependency on the server code)."""

import hashlib
import hmac
import time
from collections.abc import Mapping


def verify_webhook(body: bytes, headers: Mapping[str, str], secret: str, tolerance_seconds: int = 300) -> bool:
    """
    True if `body` (the raw request bytes) was signed with `secret` and sent
    within `tolerance_seconds` (which rejects replayed requests).

        @app.post("/hooks/job-finished")
        async def hook(request: Request):
            body = await request.body()
            if not verify_webhook(body, request.headers, os.environ["WEBHOOK_SIGNING_SECRET"]):
                raise HTTPException(401)
            event = json.loads(body)   # de-duplicate on request.headers["X-JobScheduler-Delivery"]
    """
    lowered = {k.lower(): v for k, v in headers.items()}
    timestamp = lowered.get("x-jobscheduler-timestamp", "")
    signature = lowered.get("x-jobscheduler-signature", "")
    try:
        if abs(time.time() - int(timestamp)) > tolerance_seconds:
            return False
    except ValueError:
        return False
    expected = "sha256=" + hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)
