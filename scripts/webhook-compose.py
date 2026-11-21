#!/usr/bin/env python3
"""Submit one synthetic notification through an isolated stack and verify its receipt."""
import contextlib
import datetime
import json
import subprocess
import sys
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from recovery import Experiment, require, wait_until

BASE = "https://webhook.site"


def remote(method, path, payload=None):
    request = Request(BASE + path, method=method, headers={"Content-Type": "application/json",
                      "Accept": "application/json"}, data=json.dumps(payload).encode() if payload else None)
    try:
        with urlopen(request, timeout=15) as response:
            return json.load(response) if method != "DELETE" else response.status
    except HTTPError as err:
        raise RuntimeError(f"Webhook.site {method} returned HTTP {err.code}") from None
    except (URLError, TimeoutError):
        raise RuntimeError(f"Webhook.site {method} connection failed") from None


@contextlib.contextmanager
def endpoint(message_id):
    receipt = {"messageId": message_id, "status": "accepted",
               "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    result = remote("POST", "/token", {"default_status": 202, "default_content_type": "application/json",
                    "default_content": json.dumps(receipt), "expiry": 600, "request_limit": 5, "actions": False})
    token = str(uuid.UUID(result["uuid"]))
    try:
        yield token
    finally:
        try:
            print(f"Disposable token cleanup: HTTP {remote('DELETE', '/token/' + token)}", flush=True)
        except RuntimeError as err:
            print(f"{err}. Disposable token configured to expire in 600 seconds.", file=sys.stderr)


def verify(exp, token, message_id):
    exp.env["PROVIDER_URL"] = BASE + "/" + token
    exp.compose("up", "-d", "--no-build", "--no-deps", "--scale", "worker=1", "worker")
    item = {"recipient": "assessment-smoke@example.invalid", "channel": "email", "priority": "normal",
            "content": "Synthetic notification integration smoke."}
    path = exp.api + "/v1/notifications"
    receipt = exp.request("POST", path, item, 202, {"Idempotency-Key": str(uuid.uuid4())})
    notification_id = str(uuid.UUID(receipt["id"]))

    def terminal():
        result = exp.request("GET", path + "/" + notification_id)
        if result["status"] in ("failed", "unknown", "cancelled"):
            raise AssertionError(f"Expected an accepted acknowledgement, observed {result['status']}")
        return result if result["status"] == "accepted" else None

    final = wait_until(terminal, "provider acceptance")
    require(final.get("messageId") == message_id, "persisted messageId differs from configured receipt")
    history = wait_until(lambda: remote("GET", f"/token/{token}/requests")["data"] or None, "provider history")
    require(len(history) == 1 and history[0]["method"] == "POST", "expected exactly one captured POST")
    headers = {k.lower(): v for k, v in history[0]["headers"].items()}
    require(headers.get("x-notification-id") == [notification_id], "captured notification ID differs")
    expected = {"to": item["recipient"], "channel": item["channel"], "content": item["content"]}
    require(json.loads(history[0]["content"]) == expected, "captured payload differs")
    print(f"PASS notification_id={notification_id} api_create=202 api_outcome=accepted "
          "provider_posts=1 message_id_verified=true payload_verified=true", flush=True)


def main():
    exp = Experiment()
    exp.env["APP_IMAGE"] = "notification-system:" + exp.project
    try:
        exp.compose("up", "--build", "-d", "--wait", "--scale", "worker=0")
        message_id = str(uuid.uuid4())
        with endpoint(message_id) as token:
            verify(exp, token, message_id)
    finally:
        exp.compose("down", "--volumes", "--remove-orphans")
        subprocess.run(["docker", "image", "rm", exp.env["APP_IMAGE"]], check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
