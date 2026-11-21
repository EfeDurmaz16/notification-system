"""Cross-check one live scrape using committed terminal events in the test database."""
import json
import math


def check_window(exp, read_rates):
    # During a scrape both inserts and five-minute expirations continue. Read
    # before and after it, using the database clock for both bounds. Terminal
    # rows keep the transition time in updated_at, including after a retry.
    def snapshot(aggregate, start):
        sql = f"""
            SELECT json_build_object('at', extract(epoch FROM statement_timestamp()),
                       'series', coalesce(json_object_agg(series, points), '{{}}'::json))
            FROM (
                SELECT n.channel || '_' || e.status AS series, {aggregate} AS points
                FROM status_events e JOIN notifications n ON n.id = e.notification_id
                WHERE e.status IN ('accepted','failed','unknown')
                  AND n.updated_at > {start} - interval '5 minutes'
                  AND n.updated_at <= statement_timestamp()
                GROUP BY n.channel, e.status
            ) grouped"""
        result = exp.compose("exec", "-T", "postgres", "psql", "-XAtq",
                             "-v", "ON_ERROR_STOP=1", "-U", "notify", "-d", "notify",
                             "-c", sql, capture=True)
        return json.loads(result.stdout)

    before = snapshot("array_agg(extract(epoch FROM n.updated_at))", "statement_timestamp()")
    rates = read_rates(exp)
    after = snapshot("count(*)", f"to_timestamp({float(before['at'])})")
    result = {"beforeUnix": before["at"], "afterUnix": after["at"], "series": {}}
    for channel in ("sms", "email", "push"):
        for status in ("accepted", "failed", "unknown"):
            series = f"{channel}_{status}"
            # Events already visible that cannot expire before the second read.
            lower = sum(t > after["at"] - 300 for t in before["series"].get(series, []))
            # Includes every event that could have appeared in the scrape.
            upper = after["series"].get(series, 0)
            count = rates[series] * 300
            if not math.isclose(count, round(count), rel_tol=0, abs_tol=1e-7):
                raise AssertionError(f"{series} rate did not yield a whole notification count: {count}")
            if not lower <= round(count) <= upper:
                raise AssertionError(f"{series}: {round(count)} notifications outside [{lower}, {upper}]")
            result["series"][series] = {"min": lower, "reported": round(count), "max": upper}
    return result
