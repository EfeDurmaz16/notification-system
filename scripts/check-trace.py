#!/usr/bin/env python3
"""Send one synthetic notification and verify the trace in the local Jaeger UI API."""
import json
import os
import secrets
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def main():
    api = "http://127.0.0.1:" + os.getenv("API_PORT", "8080")
    jaeger = "http://127.0.0.1:" + os.getenv("JAEGER_PORT", "16686")
    trace_id = secrets.token_hex(16)
    headers = {"Content-Type": "application/json", "Idempotency-Key": secrets.token_hex(16),
               "traceparent": "00-" + trace_id + "-" + secrets.token_hex(8) + "-01"}
    body = {"recipient": "+12025550117", "channel": "sms", "priority": "normal",
            "content": "Synthetic tracing smoke test"}
    req = Request(api + "/v1/notifications", data=json.dumps(body).encode(), headers=headers)
    with urlopen(req, timeout=5) as res:
        if res.status != 202:
            raise RuntimeError(f"Unexpected create response {res.status}")
    required = {"notification.api", "notification.process", "notification.provider"}
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            with urlopen(jaeger + "/api/traces/" + trace_id, timeout=5) as res:
                traces = json.load(res)["data"]
        except HTTPError as err:
            if err.code != 404:
                raise
            traces = []
        for trace in traces or []:
            spans = {s["operationName"]: s for s in trace["spans"]}
            if required <= spans.keys():
                for parent, child in (("notification.api", "notification.process"),
                                      ("notification.process", "notification.provider")):
                    refs = spans[child].get("references", [])
                    if not any(ref.get("refType") == "CHILD_OF" and
                               ref.get("spanID") == spans[parent]["spanID"] and
                               ref.get("traceID") == trace_id for ref in refs):
                        raise RuntimeError(f"{child} has no edge from {parent}")
                print(f"PASS {jaeger}/trace/{trace_id} ({', '.join(sorted(required))})")
                return
        time.sleep(1)
    raise SystemExit(f"Missing application spans after 60s: {jaeger}/trace/{trace_id}")


if __name__ == "__main__":
    main()
