package api

import (
	"errors"
	"io"
	"net/http"
	"sync"
)

var errBodyCapacity = errors.New("create capacity temporarily exhausted")

const initialBodyCapacity = 4 << 10

type bodyBudget struct {
	mu        sync.Mutex
	available int
}

type createBody struct {
	budget *bodyBudget
	data   []byte
}

func (b *createBody) release() {
	b.budget.mu.Lock()
	defer b.budget.mu.Unlock()
	b.budget.available += cap(b.data)
	b.data = nil
}

// Charge the final capacities of the active uploads. A resize temporarily
// retains the old allocation while copying. Serializing only that copy bounds
// this extra live memory to one old buffer, even if every upload grows at once.
func (b *createBody) grow(target int) bool {
	b.budget.mu.Lock()
	defer b.budget.mu.Unlock()
	delta := target - cap(b.data)
	if delta > b.budget.available {
		return false
	}
	buf := make([]byte, len(b.data), target)
	copy(buf, b.data)
	b.data = buf
	b.budget.available -= delta
	return true
}

// Read directly into charged storage. At an exact capacity boundary, probe
// for one byte before growing: EOF or a slow sender needs no extra capacity.
func (b *createBody) read(src io.Reader) error {
	if !b.grow(initialBodyCapacity) {
		return errBodyCapacity
	}
	var probe [1]byte
	for {
		free := b.data[len(b.data):cap(b.data)]
		target := free
		if len(free) == 0 {
			target = probe[:]
		}
		n, err := src.Read(target)
		if n > 0 && len(free) == 0 {
			if len(b.data) == maxBody {
				return &http.MaxBytesError{Limit: maxBody}
			}
			if !b.grow(min(2*cap(b.data), maxBody)) {
				return errBodyCapacity
			}
			b.data = append(b.data, probe[0])
		} else {
			b.data = b.data[:len(b.data)+n]
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
