// Package cancelablepriorityqueue provides a concurrency-safe, cancelable
// priority queue with blocking Pop and context support.
//
// Items are dequeued by ascending priority (lower value first); items with
// equal priority are dequeued strictly FIFO. Every successful Enqueue
// assigns a unique, strictly increasing uint64 ID starting at 1, which can
// be used to Cancel the item while it is still pending. The queue uses no
// internal background goroutines.
package cancelablepriorityqueue

import (
	"container/heap"
	"context"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrClosed is returned by Enqueue and Cancel after Close, and by Pop
	// once the queue is closed and fully drained.
	ErrClosed = errors.New("cancelablepriorityqueue: queue is closed")
	// ErrNotPending is returned by Cancel when the id is zero, unknown, or
	// no longer pending (already popped or already canceled).
	ErrNotPending = errors.New("cancelablepriorityqueue: id is not pending")
)

// Item is a value stored in the queue.
type Item struct {
	ID       uint64
	Priority int
	Value    string
}

// Snapshot is an independent copy of the queue state at a point in time.
type Snapshot struct {
	Items   []Item // all pending items, in exact Pop order
	NextID  uint64 // ID the next successful Enqueue will assign
	Waiting int    // number of Pop calls currently blocked
	Closed  bool   // whether Close has been called
}

// compareItems orders by priority, then by ID (ID order == enqueue order,
// which yields FIFO within the same priority).
func compareItems(a, b Item) int {
	if a.Priority != b.Priority {
		if a.Priority < b.Priority {
			return -1
		}
		return 1
	}
	if a.ID != b.ID {
		if a.ID < b.ID {
			return -1
		}
		return 1
	}
	return 0
}

// entry tracks one queued item and its position in the heap so Cancel can
// remove it in O(log n) without scanning.
type entry struct {
	item  Item
	index int
}

type entryHeap []*entry

func (h entryHeap) Len() int { return len(h) }

func (h entryHeap) Less(i, j int) bool {
	return compareItems(h[i].item, h[j].item) < 0
}

func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *entryHeap) Push(x any) {
	e := x.(*entry)
	e.index = len(*h)
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

// Queue is a concurrency-safe cancelable priority queue. Use New to create
// one; the zero value is not usable.
type Queue struct {
	mu      sync.Mutex
	h       entryHeap
	pending map[uint64]*entry
	wake    chan struct{} // closed and replaced to broadcast to blocked Pops
	nextID  uint64
	waiting int
	closed  bool
}

// New returns an empty, open Queue.
func New() *Queue {
	return &Queue{
		pending: make(map[uint64]*entry),
		wake:    make(chan struct{}),
		nextID:  1,
	}
}

// Enqueue adds value with the given priority (lower is dequeued first) and
// returns its ID. IDs are strictly increasing, start at 1, and are never
// reused. It returns ErrClosed after Close.
func (q *Queue) Enqueue(priority int, value string) (uint64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, ErrClosed
	}
	id := q.nextID
	q.nextID++
	e := &entry{item: Item{ID: id, Priority: priority, Value: value}}
	heap.Push(&q.h, e)
	q.pending[id] = e
	close(q.wake)
	q.wake = make(chan struct{})
	return id, nil
}

// Pop removes and returns the highest-priority pending item (FIFO among
// equal priorities). While the queue is empty and open, Pop blocks until an
// item is enqueued, the queue is closed, or ctx is done. A canceled ctx
// affects only this Pop call; items are never lost or delivered twice.
// After Close, Pop drains the remaining items in order and returns
// ErrClosed once the queue is empty.
func (q *Queue) Pop(ctx context.Context) (Item, error) {
	q.mu.Lock()
	for {
		if len(q.h) > 0 {
			e := heap.Pop(&q.h).(*entry)
			delete(q.pending, e.item.ID)
			q.mu.Unlock()
			return e.item, nil
		}
		if q.closed {
			q.mu.Unlock()
			return Item{}, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			q.mu.Unlock()
			return Item{}, err
		}
		ch := q.wake
		q.waiting++
		q.mu.Unlock()

		var canceled bool
		select {
		case <-ch:
		case <-ctx.Done():
			canceled = true
		}

		q.mu.Lock()
		q.waiting--
		if canceled {
			q.mu.Unlock()
			return Item{}, ctx.Err()
		}
	}
}

// Cancel removes a still-pending item by ID in O(log n). It returns
// ErrNotPending for id 0, unknown IDs, and IDs already popped or canceled,
// and ErrClosed after Close.
func (q *Queue) Cancel(id uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	e, ok := q.pending[id]
	if !ok {
		return ErrNotPending
	}
	heap.Remove(&q.h, e.index)
	delete(q.pending, id)
	return nil
}

// Snapshot returns an independent copy of the current queue state. Items
// are ordered exactly as Pop would return them.
func (q *Queue) Snapshot() Snapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	items := make([]Item, len(q.h))
	for i, e := range q.h {
		items[i] = e.item
	}
	// The comparator matches the heap ordering, so sorting a copy yields
	// the exact order in which Pop would return the items.
	sort.Slice(items, func(i, j int) bool { return compareItems(items[i], items[j]) < 0 })
	return Snapshot{
		Items:   items,
		NextID:  q.nextID,
		Waiting: q.waiting,
		Closed:  q.closed,
	}
}

// Len returns the number of pending items.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.h)
}

// Close rejects future Enqueue and Cancel calls and wakes all blocked Pops.
// Items still pending remain drainable via Pop. Close is idempotent.
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	close(q.wake)
}
