#!/usr/bin/env python3
"""Validate live HTTP bodies as examples of their published OpenAPI response schemas."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    args = parser.parse_args()
    if urlsplit(args.url).hostname not in ("localhost", "127.0.0.1", "::1"):
        parser.error("contract checks create and cancel synthetic data; supply a local API")

    cases = {}

    def capture(method, route, template, code, body=None, key=None, response_ref=None):
        headers = {"Content-Type": "application/json"}
        if key:
            headers["Idempotency-Key"] = key
        request = Request(args.url.rstrip("/") + route, method=method,
                          data=None if body is None else json.dumps(body).encode(),
                          headers=headers)
        try:
            response = urlopen(request, timeout=10)
        except HTTPError as err:
            response = err
        with response:
            raw = response.read()
            if response.status != code:
                raise SystemExit(f"{method} {route}: expected {code}, got {response.status}: {raw[:256]}")
        if code == 204:
            if raw:
                raise SystemExit("204 response carried a body")
            return
        value = json.loads(raw)
        path = template.replace("~", "~0").replace("/", "~1")
        # Responses referenced from components have their media type defined there.
        pointer = response_ref or f"paths/{path}/{method.lower()}/responses/{code}"
        cases[f"Case{len(cases)}"] = {
            "allOf": [{"$ref": f"openapi.yaml#/{pointer}/content/application~1json/schema"}],
            "example": value,
        }
        return value

    item = {"recipient": "+12025550147", "channel": "sms", "priority": "high",
            "content": "Contract check", "scheduledAt": "2099-04-15T12:00:00Z"}
    notifications = "/v1/notifications"
    batches = "/v1/batches"
    key = str(uuid4())
    receipt = capture("POST", notifications, notifications, 202, item, key)
    repeated = capture("POST", notifications, notifications, 202, item, key)
    if repeated != receipt:
        raise SystemExit("replay returned different identifiers")
    capture("POST", notifications, notifications, 409, {**item, "content": "Different"}, key,
            "components/responses/Conflict")
    path = notifications + "/" + receipt["id"]
    capture("GET", path, notifications + "/{id}", 200)
    capture("POST", path + "/cancel", notifications + "/{id}/cancel", 204)
    state = capture("GET", path, notifications + "/{id}", 200)
    if state["status"] != "cancelled":
        raise SystemExit("cancelled notification not visible")
    history = capture("GET", path + "/history", notifications + "/{id}/history", 200)
    if [event["status"] for event in history["items"]] != ["scheduled", "cancelled"]:
        raise SystemExit("notification history did not preserve both transitions in order")
    batch = capture("POST", batches, batches, 202,
                    {"notifications": [item, {**item, "recipient": "invalid"}]},
                    str(uuid4()), "components/responses/Batch")
    capture("GET", batches + "/" + batch["batchId"], batches + "/{id}", 200,
            response_ref="components/responses/Batch")
    capture("POST", notifications + "/" + batch["items"][0]["id"] + "/cancel",
            notifications + "/{id}/cancel", 204)
    capture("POST", batches, batches, 422, {"notifications": [{}]}, str(uuid4()))
    capture("GET", notifications + "?status=cancelled&limit=2", notifications, 200)
    capture("GET", "/healthz", "/healthz", 200, response_ref="components/responses/Health")
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        shutil.copyfile(ROOT / "openapi.yaml", directory / "openapi.yaml")
        fixture = {"openapi": "3.0.3", "info": {"title": "Live response contract", "version": "1"},
                   "paths": {}, "components": {"schemas": cases}}
        (directory / "captures.json").write_text(json.dumps(fixture))
        subprocess.run(["go", "run", "github.com/getkin/kin-openapi/cmd/validate@v0.153.0",
                        "--ext", "--examples", "--multi", "--", str(directory / "captures.json")],
                       check=True, cwd=ROOT)
    print(f"{len(cases)} live JSON responses matched the documented schemas; cancellation returned 204.")


if __name__ == "__main__":
    main()
