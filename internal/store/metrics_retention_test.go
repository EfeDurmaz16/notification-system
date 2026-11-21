package store

import (
	"net/url"
	"os/exec"
	"testing"
)

func TestMetricRollupTracksTransactionsAndLatencyBounds(t *testing.T) {
	s := testStore(t)
	ctx := t.Context()
	id := createOne(t, s, sms())
	run := func(sql string) {
		t.Helper()
		if _, err := s.db.ExecContext(ctx, sql, id); err != nil {
			t.Fatal(err)
		}
	}
	run(`UPDATE notifications SET status='accepted',updated_at=created_at+interval '100 milliseconds' WHERE id=$1`)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM notifications WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	m, err := s.Metrics(ctx)
	if err != nil || m.Counts["accepted"] != 1 || m.Counts["pending"] != 0 || m.LatencyBuckets[0.1] != 1 || m.LatencySumSeconds != 0.1 {
		t.Fatalf("after rollback: %+v %v", m, err)
	}
	run(`UPDATE notifications SET updated_at=created_at+interval '500 milliseconds' WHERE id=$1`)
	m, err = s.Metrics(ctx)
	if err != nil || m.LatencyBuckets[0.1] != 0 || m.LatencyBuckets[0.5] != 1 || m.LatencyCount != 1 || m.LatencySumSeconds != 0.5 {
		t.Fatalf("after correction: %+v %v", m, err)
	}
	run(`DELETE FROM notifications WHERE id=$1`)
	m, err = s.Metrics(ctx)
	if err != nil || m.LatencyCount != 0 || m.Counts["accepted"] != 0 || m.LatencyBuckets[60] != 0 {
		t.Fatalf("after delete: %+v %v", m, err)
	}
}

func TestRetentionRequiresPublicationEvidenceAndPreservesUnresolved(t *testing.T) {
	dsn := testDSN(t)
	ctx := t.Context()
	s, err := Open(ctx, dsn, ChannelRoutes{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	var published, unpublished, unresolved, pending string
	for i, state := range []string{"accepted", "accepted", "unknown", "pending"} {
		id := createOne(t, s, sms())
		if _, err := s.db.ExecContext(ctx, `UPDATE notifications SET status=$2,updated_at=now()-interval '3 days' WHERE id=$1`, id, state); err != nil {
			t.Fatal(err)
		}
		out := []*string{&published, &unpublished, &unresolved, &pending}[i]
		if err := s.db.QueryRowContext(ctx, `UPDATE outbox SET created_at=now()-interval '3 days' WHERE aggregateid=$1 RETURNING id`, id).Scan(out); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE status_events SET changed_at=now()-interval '3 days'`); err != nil {
		t.Fatal(err)
	}
	connection, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := connection.Query()
	query.Set("options", "-csearch_path="+query.Get("search_path"))
	query.Del("search_path")
	connection.RawQuery = query.Encode()
	cleanup := func(apply string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "psql", connection.String(), "-Xq", "-v", "before=yesterday", "-v", "apply="+apply, "-v", "published_ids="+published+","+unresolved+","+pending, "-f", "../../scripts/retention.sql")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("psql: %s: %v", out, err)
		}
	}
	check := func(wantOut, wantEvents int) {
		t.Helper()
		var out, events, keep, notifications, batches int
		err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM outbox), (SELECT count(*) FROM status_events),
		    (SELECT count(*) FROM outbox WHERE id IN ($1,$2,$3)), (SELECT count(*) FROM notifications), (SELECT count(*) FROM batches)`, unpublished, unresolved, pending).Scan(&out, &events, &keep, &notifications, &batches)
		if err != nil || out != wantOut || events != wantEvents || keep != 3 || notifications != 4 || batches != 4 {
			t.Fatalf("out=%d events=%d keep=%d notifications=%d batches=%d err=%v", out, events, keep, notifications, batches, err)
		}
	}
	cleanup("false")
	check(4, 7)
	cleanup("true")
	check(3, 3)
	cleanup("true")
	check(3, 3)
}
