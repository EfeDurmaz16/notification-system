package delivery

import (
	"context"
	"net/http"
	"testing"
	"time"
)

type shutdownDuringClaimReply struct {
	recordStore
	shutdown context.CancelFunc
	deadline time.Time
}

func (s *shutdownDuringClaimReply) Start(ctx context.Context, _ string, _ int) (bool, error) {
	// Model a committed UPDATE whose acknowledgement is still in transit.
	s.state = "sending"
	s.deadline, _ = ctx.Deadline()
	s.shutdown()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, nil
}

func TestShutdownWhileAwaitingClaimReplyDrainsOnePOST(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	ctx, stop := context.WithDeadline(t.Context(), deadline)
	defer stop()
	store := &shutdownDuringClaimReply{shutdown: stop}
	posts := 0
	client := &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		posts++
		return acceptedTransport{}.RoundTrip(r)
	})}
	engine := Engine{Store: store, Client: client, URL: "http://provider.invalid"}
	if err := engine.Send(ctx, initialJob()); err != nil {
		t.Fatalf("claim reply lost during shutdown: %v; state=%s, POSTs=%d", err, store.state, posts)
	}
	if !store.deadline.Equal(deadline) || posts != 1 || store.state != accepted || store.messageID != "m1" {
		t.Fatalf("claim deadline=%v, POSTs=%d, state=%s, receipt=%q", store.deadline, posts, store.state, store.messageID)
	}
}
