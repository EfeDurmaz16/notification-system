package api

import (
	"errors"
	"io"
	"sync"
)

var errBodyCapacity = errors.New("create capacity temporarily exhausted")

// The budget covers the capacity of encoded body buffers, including scratch
// space and both buffers during growth. Parsing and storage have separate costs.
type bodyBudget struct {
	mu        sync.Mutex
	available int
}

func (b *bodyBudget) take(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.available {
		return false
	}
	b.available -= n
	return true
}

func (b *bodyBudget) give(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.available += n
}

type createBody struct {
	budget *bodyBudget
	data   []byte
}

func (b *createBody) release() {
	b.budget.give(cap(b.data))
	b.data = nil
}

// Only this small read buffer is reserved while waiting for the first bytes.
// Declared lengths and chunked transfer use the same incremental accounting.
func (b *createBody) read(src io.Reader) error {
	const chunk = 4 << 10
	if !b.budget.take(chunk) {
		return errBodyCapacity
	}
	defer b.budget.give(chunk)
	var scratch [chunk]byte
	_, err := io.CopyBuffer(b, src, scratch[:])
	return err
}

func (b *createBody) Write(p []byte) (int, error) {
	needed := len(b.data) + len(p)
	if needed > cap(b.data) {
		target := min(maxBody, max(needed, 2*cap(b.data)))
		if !b.budget.take(target) {
			return 0, errBodyCapacity
		}
		replacement := make([]byte, len(b.data), target)
		copy(replacement, b.data)
		oldCapacity := cap(b.data)
		b.data = replacement
		b.budget.give(oldCapacity)
	}
	b.data = append(b.data, p...)
	return len(p), nil
}
