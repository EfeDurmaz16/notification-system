#!/usr/bin/env python3
"""Compare a late high SMS with a co-admitted low SMS behind 10,000 low jobs."""
import concurrent.futures
import datetime
import json
import pathlib
import subprocess
import time
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
API = "http://127.0.0.1:18080"
PROVIDER = "http://127.0.0.1:8090"


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def http(url, body=None):
    headers = {"Content-Type": "application/json", "Idempotency-Key": str(uuid.uuid4())}
    data = None if body is None else json.dumps(body).encode()
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=15) as response:
        return json.load(response)


def sql(query):
    return command("docker", "compose", "exec", "-T", "postgres", "psql", "-U", "notify", "-d", "notify",
                   "-XAtq", "-v", "ON_ERROR_STOP=1", "-c", query)


def item(priority, text):
    return {"channel": "sms", "priority": priority, "recipient": "+12025550123", "content": text}


def wait_for(check, seconds=240):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.1)
    raise TimeoutError("test did not reach the required state")


def sql_ids(values):
    return ",".join("'" + str(uuid.UUID(value)) + "'" for value in values)


def run(trial):
    assert int(sql("SELECT count(*) FROM notifications WHERE status IN ('pending','sending','retry_wait')")) == 0
    idle = http(API + "/v1/notifications", item("high", "Idle control"))["id"]
    wait_for(lambda: http(API + "/v1/notifications/" + idle)["status"] == "accepted")
    time.sleep(1.1)
    before = http(PROVIDER + "/stats")["attempts"]
    def create_batch(n):
        result = http(API + "/v1/batches", {"notifications": [item("low", f"Campaign {trial}/{n}/{i}") for i in range(1000)]})
        assert len(result["items"]) == 1000 and all(i.get("id") for i in result["items"])
        return result["batchId"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        campaign = list(pool.map(create_batch, range(10)))
    wait_for(lambda: http(PROVIDER + "/stats")["attempts"] - before >= 500)
    pair = http(API + "/v1/batches", {"notifications": [item("high", "Security alert"), item("low", "Paired campaign probe")]})
    high, low = [x["id"] for x in pair["items"]]
    wait_for(lambda: http(API + "/v1/notifications/" + high)["status"] == "accepted")
    print(f"Trial {trial}: urgent accepted; waiting for campaign to drain.", flush=True)
    wait_for(lambda: http(PROVIDER + "/stats")["attempts"] - before >= 10002)
    where = "batch_id IN (" + sql_ids(campaign) + ")"
    wait_for(lambda: sql("SELECT count(*) FROM notifications WHERE status='accepted' AND " + where) == "10000")
    wait_for(lambda: http(API + "/v1/notifications/" + low)["status"] == "accepted")
    result = json.loads(sql(f"""
        WITH p AS (SELECT * FROM notifications WHERE id IN ({sql_ids([idle,high,low])})),
        h AS (SELECT * FROM p WHERE id='{high}'), c AS (SELECT * FROM notifications WHERE {where})
        SELECT json_build_object('trial',{trial},
          'campaignOutstandingAtProbeCreate',(SELECT count(*) FROM c,h WHERE c.updated_at>h.created_at),
          'campaignStillAcceptedLaterThanHigh',(SELECT count(*) FROM c,h WHERE c.updated_at>h.updated_at),
          'campaignLastMinusProbeCreateSec',(SELECT extract(epoch FROM max(c.updated_at)-min(h.created_at)) FROM c,h),
          'probeTimes',(SELECT json_object_agg(id,json_build_object('status',status,
            'queueMs',1000*extract(epoch FROM first_attempt_at-created_at),
            'sendMs',1000*extract(epoch FROM updated_at-first_attempt_at),
            'totalMs',1000*extract(epoch FROM updated_at-created_at))) FROM p))
    """))
    result["probes"] = {name: dict(result["probeTimes"][i], id=i, posts=http(PROVIDER + "/stats/" + i)["attempts"])
                        for name,i in (("idle",idle),("high",high),("low",low))}
    del result["probeTimes"]
    result["postCountDuringLoadedPhase"] = http(PROVIDER + "/stats")["attempts"] - before
    assert result["postCountDuringLoadedPhase"] == 10002
    assert all(p["posts"] == 1 and p["status"] == "accepted" for p in result["probes"].values())
    return result


if __name__ == "__main__":
    command("scripts/require-local-provider.sh")
    workers = command("docker","compose","ps","--status","running","-q","worker").splitlines()
    report = {"utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "revision":command("git","rev-parse","HEAD"),
              "workerReplicas":len(workers), "workerImage":command("docker","inspect","--format","{{.Image}}",workers[0]),
              "trials":[]}
    out = ROOT/"reports"/"urgent-probe"/"results.json"
    out.parent.mkdir(parents=True,exist_ok=True)
    for trial in range(1,4):
        result = run(trial)
        report["trials"].append(result)
        out.write_text(json.dumps(report,indent=2)+"\n")
        print(json.dumps(result),flush=True)
