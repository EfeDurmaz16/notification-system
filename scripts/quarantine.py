#!/usr/bin/env python3
"""Verify that a worker quarantines one malformed record and advances on Kafka."""
import argparse
import json
from pathlib import Path

from recovery import Experiment, require, wait_until


SOURCE = "notifications.normal.single"
GROUP = "delivery.normal.single"
INVALID = "notifications.invalid"


def kafka(exp, command, *args):
    return exp.compose("exec", "-T", "kafka", f"/opt/kafka/bin/kafka-{command}.sh",
                       "--bootstrap-server", "kafka:9092", *args, capture=True).stdout.strip()


def expect_committed(exp, expected):
    def check():
        output = kafka(exp, "consumer-groups", "--describe", "--offsets", "--group", GROUP)
        for line in output.splitlines():
            fields = line.split()
            if fields[:3] == [GROUP, SOURCE, "0"] and fields[3:6] == [str(expected), str(expected), "0"]:
                return {"committed": expected, "end": expected, "lag": 0}
        return None

    return wait_until(check, f"partition 0 commits offset {expected} with zero lag")


def exercise(exp):
    exp.compose("up", "-d", "--wait", "kafka")
    # Force the poison record and the valid CDC event onto the same partition.
    # The normal topics bootstrap preserves this pre-created fixture topic.
    kafka(exp, "topics", "--create", "--topic", SOURCE, "--partitions", "1", "--replication-factor", "1")
    exp.compose("up", "--build", "-d", "--wait")

    # An invalid ID makes the ready envelope unusable. Neither key nor value
    # in the quarantine topic should contain the synthetic contact marker.
    payload = '{"id":"synthetic-recipient-marker","attempt":1}'
    exp.compose("exec", "-T", "kafka", "sh", "-ec",
                'printf "%s\\n" "$1" | /opt/kafka/bin/kafka-console-producer.sh '
                '--bootstrap-server kafka:9092 --producer-property acks=all --topic "$2"',
                "producer", payload, SOURCE)
    expected_ref = f"{SOURCE}/0/0"
    marker = kafka(exp, "console-consumer", "--topic", INVALID, "--partition", "0", "--offset", "0",
                   "--max-messages", "1", "--timeout-ms", "45000", "--property", "print.key=true")
    require(marker == f"{expected_ref}\t{expected_ref}", f"unexpected quarantine key/value: {marker!r}")
    first_commit = expect_committed(exp, 1)
    require(exp.stats()["attempts"] == 0, "malformed record reached the provider")

    item = exp.create("valid notification after a quarantined Kafka record")
    exp.expect(item, "accepted")
    second_commit = expect_committed(exp, 2)
    event = json.loads(kafka(exp, "console-consumer", "--topic", SOURCE, "--partition", "0", "--offset", "1",
                             "--max-messages", "1", "--timeout-ms", "15000"))
    if isinstance(event, str):
        event = json.loads(event)
    require(event["id"] == item and event["attempt"] == 1, "offset 1 is not the valid API notification")
    count = exp.request("GET", exp.provider + "/stats/" + item)["attempts"]
    require(count == exp.stats()["attempts"] == 1, "expected exactly one provider POST for the valid ID")
    invalid_end = kafka(exp, "get-offsets", "--topic", INVALID, "--time", "latest")
    require(invalid_end == f"{INVALID}:0:1", f"unexpected quarantine end offset: {invalid_end}")
    return {"result": "pass", "sourceTopic": SOURCE, "partition": 0, "consumerGroup": GROUP,
            "markerKey": expected_ref, "markerValue": expected_ref, "quarantineRecords": 1,
            "afterMalformed": {"sourceOffset": 0, "providerPosts": 0, **first_commit},
            "afterValid": {"sourceOffset": 1, "notificationId": item, "status": "accepted",
                           "providerPostsForId": count, "totalProviderPosts": 1, **second_commit}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path(".local/results/quarantine.json"))
    args = parser.parse_args()
    exp = Experiment()
    exp.env["APP_IMAGE"] = "notification-system:quarantine-test"
    try:
        result = exercise(exp)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result), flush=True)
    finally:
        exp.compose("down", "--volumes", "--remove-orphans")
