package store

import (
	"net/url"
	"os/exec"
	"strings"
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
	connection := retentionConnection(t, dsn)
	cleanup := func(apply, cutoff string) {
		t.Helper()
		cmd := exec.CommandContext(ctx, "psql", connection, "-Xq", "-v", cutoff, "-v", "apply="+apply, "-v", "published_ids="+published+","+unresolved+","+pending, "-f", "../../scripts/retention.sql")
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
	for _, cutoff := range []string{"keep_days=0", "keep_days=-1", "keep_days=nonsense"} {
		cmd := exec.CommandContext(ctx, "psql", connection, "-Xq", "-v", cutoff, "-v", "apply=true", "-f", "../../scripts/retention.sql")
		if out, err := cmd.CombinedOutput(); err == nil {
			t.Fatalf("unsafe retention horizon %s accepted: %s", cutoff, out)
		}
	}
	check(4, 7)
	cleanup("false", "keep_days=1")
	check(4, 7)
	cleanup("true", "before=yesterday")
	check(3, 3)
	cleanup("true", "keep_days=1")
	check(3, 3)
}

func retentionConnection(t *testing.T, dsn string) string {
	t.Helper()
	connection, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := connection.Query()
	query.Set("options", "-csearch_path="+query.Get("search_path"))
	query.Del("search_path")
	connection.RawQuery = query.Encode()
	return connection.String()
}

func TestRetentionScansPastUnresolvedPrefixAndRevisitsIt(t *testing.T) {
	dsn := testDSN(t)
	s, err := Open(t.Context(), dsn, ChannelRoutes{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	pending := createOne(t, s, sms())
	final := createOne(t, s, sms())
	mustExec := func(query string, args ...any) {
		t.Helper()
		if _, err := s.db.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	mustExec("UPDATE notifications SET status='accepted',updated_at='2020-01-01' WHERE id=$1", final)
	mustExec("DELETE FROM status_events")
	mustExec(`INSERT INTO status_events (notification_id,attempt,status,changed_at)
		SELECT $1,1,'pending','2020-01-01' FROM generate_series(1,1000)`, pending)
	mustExec(`INSERT INTO status_events (notification_id,attempt,status,changed_at)
		VALUES ($1,1,'accepted','2020-01-02')`, final)

	connection := retentionConnection(t, dsn)
	run := func(apply string) (string, error) {
		cmd := exec.CommandContext(t.Context(), "psql", connection, "-XAtq",
			"-v", "apply="+apply, "-v", "before=2021-01-01", "-f", "../../scripts/retention.sql")
		out, err := cmd.CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	check := func(apply, want string) {
		t.Helper()
		got, err := run(apply)
		if err != nil || got != want {
			t.Fatalf("apply=%s: want %q, got %q, err=%v", apply, want, got, err)
		}
	}
	check("false", "0|1000|0")
	check("false", "0|1|1")
	check("true", "0|1000|0")

	// An exception after deletion rolls back both the deletion and its cursor.
	mustExec(`CREATE FUNCTION fail_cursor() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'injected cursor failure'; END $$;
		CREATE TRIGGER fail_cursor BEFORE UPDATE ON retention_cursors FOR EACH ROW EXECUTE FUNCTION fail_cursor()`)
	if got, err := run("true"); err == nil || !strings.Contains(got, "injected cursor failure") {
		t.Fatalf("expected failure after deleting page: %s, %v", got, err)
	}
	mustExec("DROP TRIGGER fail_cursor ON retention_cursors")
	check("true", "0|1|1")
	mustExec("UPDATE notifications SET status='accepted',updated_at='2020-01-03' WHERE id=$1", pending)

	// Concurrent runs serialize on the cursor. The first handles the 1,000
	// formerly ineligible rows; the second finishes the now empty sweep.
	results := make(chan string, 2)
	for range 2 {
		go func() {
			got, err := run("true")
			if err != nil {
				got += err.Error()
			}
			results <- got
		}()
	}
	a, b := <-results, <-results
	if min(a, b) != "0|0|0" || max(a, b) != "0|1000|1000" {
		t.Fatalf("unexpected concurrent pages %q, %q", a, b)
	}
}
