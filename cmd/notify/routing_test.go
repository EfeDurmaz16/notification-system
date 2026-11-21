package main

import (
	"testing"

	"github.com/EfeDurmaz16/notification-system/internal/store"
)

func TestRoutingModesCanDrainOlderTopicsBeforeWritingNewOnes(t *testing.T) {
	shared := store.ChannelRoutes{}
	batch := store.ChannelRoutes{Batch: true}
	both := store.ChannelRoutes{Batch: true, Single: true}
	for _, tc := range []struct {
		mode          string
		writes, reads store.ChannelRoutes
	}{
		{"shared", shared, shared},
		{"dual-read", shared, batch},
		{"channel", batch, batch},
		{"read-isolated", batch, both},
		{"isolated", both, both},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			writes, reads, err := routing(tc.mode)
			if err != nil || writes != tc.writes || reads != tc.reads {
				t.Fatalf("got writes=%+v reads=%+v err=%v", writes, reads, err)
			}
		})
	}
	if _, _, err := routing("typo"); err == nil {
		t.Fatal("unknown routing mode accepted")
	}
}
