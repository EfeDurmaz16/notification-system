#!/usr/bin/env python3
"""Exercise failures in a fresh, isolated Compose stack and remove its volumes."""
import argparse
import collections
import datetime
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def wait_until(check, label, seconds=90):
    end = time.monotonic() + seconds
    last = None
    while time.monotonic() < end:
        try:
            last = check()
            if last:
                return last
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            last = str(error)
        time.sleep(0.25)
    raise TimeoutError(f"{label}: condition unmet after {seconds}s (last={last!r})")


def free_ports(count):
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


class Experiment:
    def __init__(self):
        self.project = "notify-recovery-" + uuid.uuid4().hex[:10]
        names = ["API_PORT", "PROVIDER_PORT", "CONNECT_PORT", "POSTGRES_PORT", "REDIS_PORT", "KAFKA_PORT"]
        ports = dict(zip(names, map(str, free_ports(len(names)))))
        self.env = {**os.environ, **ports, "PROVIDER_URL": "http://provider:8081/send",
                    "PROVIDER_IDEMPOTENT": "false", "OTEL_EXPORTER_OTLP_ENDPOINT": "",
                    "COMPOSE_PROFILES": "", "WORKER_CONCURRENCY": "16"}
        self.api = "http://127.0.0.1:" + ports["API_PORT"]
        self.provider = "http://127.0.0.1:" + ports["PROVIDER_PORT"]
        self.records = []

    def compose(self, *args, capture=False, check=True):
        return subprocess.run(["docker", "compose", "-f", str(ROOT / "docker-compose.yml"),
                               "-p", self.project, *args], cwd=ROOT, env=self.env, check=check,
                              capture_output=capture, text=True, timeout=600)

    def request(self, method, url, body=None, expected=200, headers=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(url, data, method=method,
                                         headers={"Content-Type": "application/json", **(headers or {})})
        with urllib.request.urlopen(request, timeout=10) as response:
            require(response.status == expected, f"{method} {url}: {response.status}, expected {expected}")
            buf = response.read()
            return json.loads(buf) if buf else None

    def stats(self):
        return self.request("GET", self.provider + "/stats")

    @staticmethod
    def item(content):
        return {"recipient": "+15550100001", "channel": "sms", "priority": "normal", "content": content}

    def create(self, content):
        response = self.request("POST", self.api + "/v1/notifications", self.item(content), 202,
                                {"Idempotency-Key": str(uuid.uuid4())})
        return response["id"]

    def get_state(self, item):
        return self.request("GET", self.api + "/v1/notifications/" + item)["status"]

    def expect(self, item, state):
        wait_until(lambda: self.get_state(item) == state, f"notification {item} to reach {state}")

    def check_outage(self, service):
        before = self.stats()["attempts"]
        self.compose("stop", service)
        item = self.create("accepted while " + service + " unavailable")
        self.expect(item, "pending")
        cancelled = None
        if service == "redis":
            cancelled = self.create("cancel during authorization outage")
            self.request("POST", self.api + "/v1/notifications/" + cancelled + "/cancel", expected=204)
        time.sleep(2)
        require(self.stats()["attempts"] == before, "provider received work during outage")
        self.compose("start", service)
        self.expect(item, "accepted")
        if cancelled:
            self.expect(cancelled, "cancelled")
        require(self.stats()["attempts"] == before + 1, "outage recovery duplicated the send")
        return {"acceptedDuringOutage": 1, "providerPOSTsDuringOutage": 0, "providerPOSTsAfterRecovery": 1}

    def cancel(self):
        self.compose("stop", "worker")
        before = self.stats()["attempts"]
        keep = self.create("survives worker restart")
        cancel = self.create("cancelled before worker restart")
        url = self.api + "/v1/notifications/" + cancel
        self.request("POST", url + "/cancel", expected=204)
        self.compose("start", "worker")
        self.expect(keep, "accepted")
        self.expect(cancel, "cancelled")
        time.sleep(1)
        require(self.stats()["attempts"] - before == 1, "cancelled work reached provider")
        return {"accepted": 1, "cancelled": 1, "providerPOSTsForBoth": 1}

    def provider_outage(self):
        self.compose("stop", "provider")
        exhaust = self.create("transient connection failure exhausts retry budget")
        self.expect(exhaust, "failed")
        exhausted = self.request("GET", self.api + "/v1/notifications/" + exhaust)
        require(exhausted["attempt"] > 1, "dial failure did not retry")
        recover = self.create("provider recovers before retry budget expires")
        self.expect(recover, "retry_wait")
        self.compose("start", "provider")
        self.expect(recover, "accepted")
        recovered = self.request("GET", self.api + "/v1/notifications/" + recover)
        require(recovered["attempt"] > 1, "accepted before retry")
        require(self.stats()["attempts"] == 1, "unexpected provider POST count after restart")
        return {"exhaustedStatus": "failed", "exhaustedAttempts": exhausted["attempt"],
                "recoveredStatus": "accepted", "recoveredAttempts": recovered["attempt"],
                "providerPOSTsAfterRestart": 1}

    def outcomes(self):
        before = self.stats()
        reject = self.create("[mock:reject] fixture refuses the recipient")
        unknown = self.create("[mock:unknown] response lost after provider accepted")
        self.expect(reject, "failed")
        self.expect(unknown, "unknown")
        time.sleep(2)
        after = self.stats()
        changes = {key: after[key] - before[key] for key in ["attempts", "accepted", "failures", "responseLost"]}
        require(changes == {"attempts": 2, "accepted": 1, "failures": 1, "responseLost": 1}, changes)
        return {"apiStates": ["failed", "unknown"], "provider": changes}

    def crash_after_acceptance(self):
        self.env["PROVIDER_LATENCY"] = "4s"
        self.compose("up", "-d", "--no-build", "--no-deps", "--force-recreate", "provider")
        wait_until(lambda: self.request("GET", self.provider + "/health"), "slow provider readiness")
        item = self.create("provider accepts, worker dies before recording response")
        wait_until(lambda: self.stats()["accepted"] == 1, "provider accepts POST")
        require(self.get_state(item) == "sending", "crash timing missed in-flight send")
        self.compose("kill", "-s", "SIGKILL", "worker")
        require(self.get_state(item) == "sending", "worker persisted response before crash")
        self.compose("start", "worker")
        wait_until(lambda: self.get_state(item) == "unknown", "abandoned send becomes unknown", seconds=155)
        fresh = self.create("emergency alert for same recipient after unknown")
        self.expect(fresh, "accepted")
        require(self.stats()["attempts"] == 2, "lost-response POST replayed after worker crash")
        return {"crashedWorkers": 2, "observedBeforeKill": "sending", "abandonedStatus": "unknown",
                "sameRecipientSubsequentStatus": "accepted", "totalProviderPOSTs": 2}

    def idempotent(self):
        self.env.update({"PROVIDER_LATENCY": "20ms", "PROVIDER_IDEMPOTENT": "true"})
        self.compose("up", "-d", "--no-build", "--no-deps", "--force-recreate", "--scale", "worker=2",
                     "provider", "worker")
        wait_until(lambda: self.request("GET", self.provider + "/health"), "idempotent provider readiness")
        item = self.create("[mock:unknown] recover through stable idempotency key")
        self.expect(item, "accepted")
        outcome = self.stats()
        require(outcome["attempts"] == 2 and outcome["accepted"] == 1 and outcome["deduplicated"] == 1,
                outcome)
        return {"providerPOSTs": 2, "providerAcceptances": 1, "deduplicated": 1, "apiState": "accepted"}

    def quota(self):
        replicas = len(self.compose("ps", "-q", "--status", "running", "worker", capture=True).stdout.splitlines())
        require(replicas == 2, f"expected two live workers, found {replicas}")
        self.request("DELETE", self.provider + "/stats", expected=204)
        size = 300
        res = self.request("POST", self.api + "/v1/batches",
                           {"notifications": [self.item("burst " + str(i)) for i in range(size)]},
                           202, {"Idempotency-Key": str(uuid.uuid4())})
        url = self.api + "/v1/batches/" + res["batchId"]
        def drained():
            states = collections.Counter(row.get("status") for row in self.request("GET", url)["items"])
            return states == {"accepted": size}
        wait_until(drained, "two-worker burst to drain")
        stats = self.stats()
        peak = stats["channels"]["sms"]["peakPerSecond"]
        require(stats["attempts"] == size and 0 < peak <= 100, stats)
        return {"runningWorkerReplicas": replicas, "accepted": size, "providerPOSTs": stats["attempts"],
                "maxProviderArrivalsInAnyRollingSecond": peak}

    def run_case(self, label, action):
        start = time.monotonic()
        record = {"case": label}
        self.records.append(record)
        try:
            record.update(action())
            record["result"] = "pass"
        except Exception as err:
            record.update({"result": "fail", "error": str(err)})
            raise
        finally:
            record["elapsedSeconds"] = round(time.monotonic() - start, 3)
            print(json.dumps(record), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--crash", action="store_true", help="run slower abrupt worker death and idempotent replay experiments instead")
    parser.add_argument("--build", action="store_true", help="build the local app image before startup")
    args = parser.parse_args()
    if args.report is None:
        args.report = ROOT / "reports" / ("recovery-crash.json" if args.crash else "recovery.json")
    exp = Experiment()
    report = {"startedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(), "cases": exp.records,
              "environment": "isolated local Docker Compose project, synthetic provider, 2 worker replicas"}
    try:
        exp.compose("up", "-d", "--build" if args.build else "--no-build", "--scale", "worker=2")
        warm = exp.create("stack readiness check")
        exp.expect(warm, "accepted")
        if args.crash:
            exp.run_case("worker process dies after provider accepts", exp.crash_after_acceptance)
            exp.run_case("idempotent provider replays the first receipt", exp.idempotent)
        else:
            exp.run_case("worker stopped, queued work cancelled", exp.cancel)
            for service in ("connect", "kafka", "redis"):
                exp.run_case(service + " interruption and recovery", lambda svc=service: exp.check_outage(svc))
            exp.run_case("dial failures retry, exhaust or recover", exp.provider_outage)
            exp.run_case("provider rejects and loses response", exp.outcomes)
            exp.run_case("strict channel quota across two workers", exp.quota)
        report["result"] = "pass"
    except Exception as err:
        report.update({"result": "fail", "error": str(err)})
        exp.compose("ps", "-a", check=False)
        print(err, file=sys.stderr)
    finally:
        exp.compose("down", "--volumes", "--remove-orphans", check=False)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["result"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
