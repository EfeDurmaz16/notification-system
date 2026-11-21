package api

import (
	"context"
	"sync"
	"time"

	"github.com/EfeDurmaz16/notification-system/internal/store"
)

type snapshot struct {
	notification store.Notification
	err          error
}

type snapshotFeed struct {
	repo        Repository
	mu          sync.Mutex
	subscribers map[chan snapshot]string
	running     bool
}

func newSnapshotFeed(repo Repository) *snapshotFeed {
	return &snapshotFeed{repo: repo, subscribers: make(map[chan snapshot]string)}
}

func (f *snapshotFeed) subscribe(id string) (<-chan snapshot, func()) {
	updates := make(chan snapshot, 1)
	f.mu.Lock()
	f.subscribers[updates] = id
	if !f.running {
		f.running = true
		go f.run()
	}
	f.mu.Unlock()
	return updates, func() {
		f.mu.Lock()
		delete(f.subscribers, updates)
		f.mu.Unlock()
	}
}

func (f *snapshotFeed) run() {
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for range tick.C {
		f.mu.Lock()
		if len(f.subscribers) == 0 {
			f.running = false
			f.mu.Unlock()
			return
		}
		listeners := make(map[string][]chan snapshot)
		for ch, id := range f.subscribers {
			listeners[id] = append(listeners[id], ch)
		}
		f.mu.Unlock()

		ids := make([]string, 0, len(listeners))
		for id := range listeners {
			ids = append(ids, id)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		items, err := f.repo.Snapshots(ctx, ids)
		cancel()
		for id, sockets := range listeners {
			n, ok := items[id]
			update := snapshot{notification: n, err: err}
			if !ok && err == nil {
				update.err = store.ErrNotFound
			}
			for _, ch := range sockets {
				// This is the sole sender. Replace any unread snapshot so a slow
				// connection cannot stall the rest of the replica's subscribers.
				select {
				case <-ch:
				default:
				}
				ch <- update
			}
		}
	}
}
