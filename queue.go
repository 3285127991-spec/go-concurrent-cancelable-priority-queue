// Package cancelablepriorityqueue provides a concurrency-safe, cancelable
// priority queue with blocking Pop semantics. It uses no internal
// background goroutines and no third-party dependencies.
package cancelablepriorityqueue

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"slices"
	"sync"
)

var (
	// ErrClosed is returned by Enqueue and Cancel after Close, and by Pop
	// once the queue is closed and fully drained.
	ErrClosed = errors.New("cancelablepriorityqueue: queue is closed")
	// ErrInvalidID is returned by Cancel when id is zero.
	ErrInvalidID = errors.New("cancelablepriorityqueue: id must be >= 1")
	// ErrNotPending is returned by Cancel when id is unknown or no longer
	// pending (already popped or already canceled).
	ErrNotPending = errors.New("cancelablepriorityqueue: id is not pending")
)

// Item is a single queued element.
type Item struct {
	ID       uint64
	Priority int
	Value    string
}

// Snapshot is an independent copy of the queue state at a point in time.
type Snapshot struct {
	// Items holds all pending items in the exact order Pop would return them.
	Items []Item
	// NextID is the id the next successful Enqueue will assign.
	NextID uint64
	// Waiting is the number of Pop calls currently blocked.
	Waiting int
	// Closed reports whether Close has been called.
	Closed bool
}

// entry is the internal heap node. heapIndex is maintained by container/heap
// so Cancel can remove in O(log n) without scanning.
type entry struct {
	item      Item
	heapIndex int
}

// entryHeap is a min-heap ordered by (Priority, ID). IDs are strictly
// increasing with enqueue order, so the ID tie-break yields strict FIFO
// within the same priority.
type entryHeap []*entry

func (h entryHeap) Len() int { return len(h) }

func (h entryHeap) Less(i, j int) bool {
	a, b := h[i].item, h[j].item
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	return a.ID < b.ID
}

func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *entryHeap) Push(x any) {
	e := x.(*entry)
	e.heapIndex = len(*h)
	*h = append(*h, e)
}

func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

// Queue is a concurrency-safe cancelable priority queue.
// The zero value is ready to use.
type Queue struct {
	mu      sync.Mutex
	heap    entryHeap
	pending map[uint64]*entry
	nextID  uint64
	waiting int
	closed  bool
	// wake is closed to broadcast a state change (new item or Close) to all
	// blocked Pop callers, then replaced with a fresh channel.
	wake chan struct{}
}

// New returns an empty, open Queue.
func New() *Queue {
	return &Queue{
		pending: make(map[uint64]*entry),
		nextID:  1,
		wake:    make(chan struct{}),
	}
}

func (q *Queue) initLocked() {
	if q.wake == nil {
		q.pending = make(map[uint64]*entry)
		q.nextID = 1
		q.wake = make(chan struct{})
	}
}

// broadcastLocked wakes every blocked Pop. Caller must hold q.mu.
func (q *Queue) broadcastLocked() {
	close(q.wake)
	q.wake = make(chan struct{})
}

// Enqueue adds value with the given priority (lower values pop first) and
// returns its id. Ids start at 1, are strictly increasing, and never repeat.
// It returns ErrClosed after Close.
func (q *Queue) Enqueue(priority int, value string) (uint64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.initLocked()
	if q.closed {
		return 0, ErrClosed
	}
	id := q.nextID
	q.nextID++
	e := &entry{item: Item{ID: id, Priority: priority, Value: value}}
	heap.Push(&q.heap, e)
	q.pending[id] = e
	q.broadcastLocked()
	return id, nil
}

// Pop atomically removes and returns the pending item with the lowest
// priority, breaking ties by enqueue order (FIFO). It blocks while the
// queue is empty and open. It returns ctx.Err() if ctx is canceled while
// waiting, and ErrClosed once the queue is closed and fully drained.
// Cancellation of ctx only affects this call; no item is lost or delivered
// twice.
func (q *Queue) Pop(ctx context.Context) (Item, error) {
	for {
		q.mu.Lock()
		q.initLocked()
		if len(q.heap) > 0 {
			e := heap.Pop(&q.heap).(*entry)
			delete(q.pending, e.item.ID)
			q.mu.Unlock()
			return e.item, nil
		}
		if q.closed {
			q.mu.Unlock()
			return Item{}, ErrClosed
		}
		ch := q.wake
		q.waiting++
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			q.mu.Lock()
			q.waiting--
			q.mu.Unlock()
			return Item{}, ctx.Err()
		case <-ch:
			q.mu.Lock()
			q.waiting--
			q.mu.Unlock()
		}
	}
}

// Cancel removes the pending item with the given id in O(log n). It returns
// ErrInvalidID for id 0, ErrNotPending for unknown or no-longer-pending ids,
// and ErrClosed after Close.
func (q *Queue) Cancel(id uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.initLocked()
	if q.closed {
		return ErrClosed
	}
	if id == 0 {
		return ErrInvalidID
	}
	e, ok := q.pending[id]
	if !ok {
		return ErrNotPending
	}
	heap.Remove(&q.heap, e.heapIndex)
	delete(q.pending, id)
	return nil
}

// Close rejects future Enqueue and Cancel calls and wakes all blocked Pop
// callers. Items already pending can still be popped in order; Pop returns
// ErrClosed once the queue is drained. Close is idempotent.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.initLocked()
	if q.closed {
		return
	}
	q.closed = true
	q.broadcastLocked()
}

// Len returns the number of pending items.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.initLocked()
	return len(q.heap)
}

// Snapshot returns an independent copy of the current state. Items are
// listed in the exact order Pop would return them.
func (q *Queue) Snapshot() Snapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.initLocked()
	items := make([]Item, 0, len(q.heap))
	for _, e := range q.heap {
		items = append(items, e.item)
	}
	slices.SortFunc(items, func(a, b Item) int {
		if c := cmp.Compare(a.Priority, b.Priority); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
	return Snapshot{
		Items:   items,
		NextID:  q.nextID,
		Waiting: q.waiting,
		Closed:  q.closed,
	}
}
