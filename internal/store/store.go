// Package store persists notification state and its transactional outbox in PostgreSQL.
package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/observability"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	ErrNotFound = errors.New("resource not found")
	ErrConflict = errors.New("operation conflicts with current state")
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct {
	db           *sql.DB
	channelBatch bool
}

func Open(ctx context.Context, dsn string, channelBatch bool) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(24)
	db.SetMaxIdleConns(12)
	db.SetConnMaxLifetime(30 * time.Minute)
	s := &Store{db: db, channelBatch: channelBatch}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}
func (s *Store) Close() error                   { return s.db.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// Serialize startup across API and worker processes, including the first DDL.
	if _, err = tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(408711735)"); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"); err != nil {
		return err
	}
	files, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	for _, file := range files {
		var exists bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)", file).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		b, err := migrations.ReadFile(file)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, string(b)); err != nil {
			return fmt.Errorf("%s: %w", file, err)
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version) VALUES($1)", file); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Ingress records the create endpoint so later attempts keep the same dispatch lane.
type Ingress string

const (
	BatchIngress  Ingress = "batch"
	SingleIngress Ingress = "single"
)

type Item struct {
	Recipient, Channel, Content, Priority, Error string
	ScheduledAt                                  *time.Time
}
type ItemResult struct {
	Index     int        `json:"index"`
	ID        string     `json:"id,omitempty"`
	Error     string     `json:"error,omitempty"`
	Status    string     `json:"status,omitempty"`
	CreatedAt *time.Time `json:"createdAt,omitempty"`
	UpdatedAt *time.Time `json:"updatedAt,omitempty"`
}
type BatchResult struct {
	BatchID string       `json:"batchId"`
	Items   []ItemResult `json:"items"`
}

func uuid() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type bulkRow struct {
	ID, Recipient, Channel, Content, Priority, Status string
	Index                                             int
	Scheduled                                         *time.Time
}

func (s *Store) Create(ctx context.Context, key string, hash []byte, ingress Ingress, items []Item) (BatchResult, error) {
	if key == "" || len(hash) == 0 || len(items) == 0 || len(items) > 1000 {
		return BatchResult{}, errors.New("invalid create envelope")
	}
	switch ingress {
	case SingleIngress:
		if len(items) != 1 {
			return BatchResult{}, errors.New("standalone ingress requires one notification")
		}
	case BatchIngress:
	default:
		return BatchResult{}, errors.New("unknown notification ingress")
	}
	res := BatchResult{BatchID: uuid(), Items: make([]ItemResult, len(items))}
	ready := make([]bulkRow, 0, len(items))
	now := time.Now()
	for i, x := range items {
		r := ItemResult{Index: i, Error: x.Error}
		if x.Error == "" {
			r.ID, r.Status = uuid(), "pending"
			if x.ScheduledAt != nil && x.ScheduledAt.After(now) {
				r.Status = "scheduled"
			}
			ready = append(ready, bulkRow{ID: r.ID, Recipient: x.Recipient, Channel: x.Channel, Content: x.Content, Priority: x.Priority, Status: r.Status, Index: i, Scheduled: x.ScheduledAt})
		}
		res.Items[i] = r
	}
	resp, err := json.Marshal(res)
	if err != nil {
		return BatchResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return BatchResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	// An INSERT conflicting with an uncommitted create waits for that transaction.
	// Under READ COMMITTED, the subsequent SELECT sees the winner's final response.
	inserted, err := tx.ExecContext(ctx, `INSERT INTO batches(id,idempotency_key,request_hash,response)
        VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(idempotency_key) DO NOTHING`, res.BatchID, key, hash, resp)
	if err != nil {
		return BatchResult{}, err
	}
	n, err := inserted.RowsAffected()
	if err != nil {
		return BatchResult{}, err
	}
	if n == 0 {
		var oldHash, oldResponse []byte
		if err = tx.QueryRowContext(ctx, "SELECT request_hash,response FROM batches WHERE idempotency_key=$1", key).Scan(&oldHash, &oldResponse); err != nil {
			return BatchResult{}, err
		}
		if !bytes.Equal(oldHash, hash) {
			return BatchResult{}, ErrConflict
		}
		var original BatchResult
		if err = json.Unmarshal(oldResponse, &original); err != nil {
			return BatchResult{}, err
		}
		return original, nil
	}
	data, err := json.Marshal(ready)
	if err != nil {
		return BatchResult{}, err
	}
	if len(ready) > 0 {
		_, err = tx.ExecContext(ctx, `WITH added AS (
            INSERT INTO notifications(id,batch_id,item_index,recipient,channel,content,priority,status,attempt,scheduled_at,next_attempt_at,traceparent,ingress,correlation_id)
            SELECT x."ID",$1,x."Index",x."Recipient",x."Channel",x."Content",x."Priority",x."Status",1,x."Scheduled",
                CASE WHEN x."Status"='scheduled' THEN x."Scheduled" END,$3,$4,$6
            FROM jsonb_to_recordset($2::jsonb) AS x("ID" uuid,"Index" int,"Recipient" text,"Channel" text,"Content" text,
                "Priority" text,"Status" text,"Scheduled" timestamptz)
            RETURNING id,priority,ingress,status,attempt,traceparent,channel,$5::boolean AS channel_batch)
            `+emitReady, res.BatchID, data, observability.Traceparent(ctx), ingress, s.channelBatch, observability.CorrelationID(ctx))
		if err != nil {
			return BatchResult{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return BatchResult{}, err
	}
	return res, nil
}

const emitReady = `INSERT INTO outbox(aggregatetype,aggregateid,type,payload)
    SELECT 'notifications.' || priority || CASE WHEN ingress='single' THEN '.single'
        WHEN channel_batch THEN '.batch.' || channel ELSE '' END,
        id,'NotificationReady',
        jsonb_build_object('id',id,'attempt',attempt,'traceparent',traceparent)
    FROM added WHERE status='pending'`
