package cancelablepriorityqueue

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func waitForWaiting(t *testing.T, q *Queue, want int) {
	t.Helper()
	for i := 0; i < 200000; i++ {
		if q.Snapshot().Waiting == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for Waiting==%d", want)
}

// checkHeapLocked verifies the heap invariant and that the id index agrees
// with the heap. Caller must not hold q.mu.
func checkHeap(t *testing.T, q *Queue) {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) != len(q.heap) {
		t.Fatalf("index/heap size mismatch: %d ids vs %d entries", len(q.pending), len(q.heap))
	}
	for i, e := range q.heap {
		if e.heapIndex != i {
			t.Fatalf("entry id=%d has heapIndex %d, want %d", e.item.ID, e.heapIndex, i)
		}
		for _, c := range []int{2*i + 1, 2*i + 2} {
			if c < len(q.heap) && q.heap.Less(c, i) {
				t.Fatalf("heap invariant violated: child %d (id=%d) orders before parent %d (id=%d)",
					c, q.heap[c].item.ID, i, e.item.ID)
			}
		}
	}
	for id, e := range q.pending {
		if e.item.ID != id || q.heap[e.heapIndex] != e {
			t.Fatalf("index entry id=%d does not point at its heap slot", id)
		}
	}
}

func popExpect(t *testing.T, q *Queue, wantValue string) Item {
	t.Helper()
	it, err := q.Pop(context.Background())
	if err != nil {
		t.Fatalf("Pop: unexpected error %v", err)
	}
	if it.Value != wantValue {
		t.Fatalf("Pop value = %q, want %q", it.Value, wantValue)
	}
	return it
}

func TestPopPriorityOrder(t *testing.T) {
	q := New()
	for _, p := range []int{5, 1, 4, 2, 3} {
		if _, err := q.Enqueue(p, fmt.Sprintf("p%d", p)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	for want := 1; want <= 5; want++ {
		it := popExpect(t, q, fmt.Sprintf("p%d", want))
		if it.Priority != want {
			t.Fatalf("Pop priority = %d, want %d", it.Priority, want)
		}
	}
	if q.Len() != 0 {
		t.Fatalf("Len = %d, want 0", q.Len())
	}
}

func TestPopSamePriorityFIFO(t *testing.T) {
	q := New()
	values := []string{"a", "b", "c", "d", "e"}
	for i, v := range values {
		id, err := q.Enqueue(7, v)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if want := uint64(i + 1); id != want {
			t.Fatalf("Enqueue id = %d, want %d", id, want)
		}
	}
	for _, v := range values {
		popExpect(t, q, v)
	}
}

func TestCancelHeapIntegrity(t *testing.T) {
	q := New()
	prios := []int{2, 1, 3, 1, 2, 3, 1, 2, 3} // ids 1..9
	for i, p := range prios {
		if _, err := q.Enqueue(p, fmt.Sprintf("id%d", i+1)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	// Cancel a middle entry, the current head-of-line, and the last entry.
	for _, id := range []uint64{5, 2, 9} {
		if err := q.Cancel(id); err != nil {
			t.Fatalf("Cancel(%d): %v", id, err)
		}
		checkHeap(t, q)
	}
	if got := q.Len(); got != 6 {
		t.Fatalf("Len = %d, want 6", got)
	}
	// Remaining ids 1(p2) 3(p3) 4(p1) 6(p3) 7(p1) 8(p2) pop as 4,7,1,8,3,6.
	for _, want := range []string{"id4", "id7", "id1", "id8", "id3", "id6"} {
		popExpect(t, q, want)
	}
	checkHeap(t, q)
}

func TestCancelInvalidOrNotPending(t *testing.T) {
	q := New()
	if err := q.Cancel(0); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Cancel(0) = %v, want ErrInvalidID", err)
	}
	if err := q.Cancel(42); !errors.Is(err, ErrNotPending) {
		t.Fatalf("Cancel(unknown) = %v, want ErrNotPending", err)
	}
	id, _ := q.Enqueue(1, "x")
	popExpect(t, q, "x")
	if err := q.Cancel(id); !errors.Is(err, ErrNotPending) {
		t.Fatalf("Cancel(popped) = %v, want ErrNotPending", err)
	}
	id2, _ := q.Enqueue(1, "y")
	if err := q.Cancel(id2); err != nil {
		t.Fatalf("Cancel(pending) = %v, want nil", err)
	}
	if err := q.Cancel(id2); !errors.Is(err, ErrNotPending) {
		t.Fatalf("Cancel(canceled) = %v, want ErrNotPending", err)
	}
}

func TestPopBlocksUntilEnqueue(t *testing.T) {
	q := New()
	type result struct {
		it  Item
		err error
	}
	res := make(chan result, 1)
	go func() {
		it, err := q.Pop(context.Background())
		res <- result{it, err}
	}()
	waitForWaiting(t, q, 1)
	id, err := q.Enqueue(3, "wake")
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	select {
	case r := <-res:
		if r.err != nil {
			t.Fatalf("Pop: %v", r.err)
		}
		if r.it.ID != id || r.it.Value != "wake" {
			t.Fatalf("Pop = %+v, want id=%d value=wake", r.it, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Pop was not woken by Enqueue")
	}
	waitForWaiting(t, q, 0)
}

func TestPopContextCancellation(t *testing.T) {
	q := New()
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() {
		_, err := q.Pop(ctx)
		res <- err
	}()
	waitForWaiting(t, q, 1)
	cancel()
	select {
	case err := <-res:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Pop error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Pop did not observe context cancellation")
	}
	// The canceled Pop must not disturb the queue.
	if _, err := q.Enqueue(1, "still-works"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	popExpect(t, q, "still-works")

	// Cancel racing Enqueue: exactly one outcome, nothing lost or duplicated.
	for i := 0; i < 10; i++ {
		rctx, rcancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := q.Pop(rctx)
			done <- err
		}()
		waitForWaiting(t, q, 1)
		go rcancel()
		if _, err := q.Enqueue(1, "raced"); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		err := <-done
		switch {
		case err == nil:
			if got := q.Len(); got != 0 {
				t.Fatalf("iter %d: successful Pop but Len = %d", i, got)
			}
		case errors.Is(err, context.Canceled):
			popExpect(t, q, "raced") // item must still be pending
		default:
			t.Fatalf("iter %d: Pop error = %v", i, err)
		}
		waitForWaiting(t, q, 0)
	}
}

func TestConcurrentPopNoDuplicates(t *testing.T) {
	q := New()
	const n = 200
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < n/4; i++ {
				if _, err := q.Enqueue(i%5, fmt.Sprintf("g%d-%d", g, i)); err != nil {
					t.Errorf("Enqueue: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()
	if got := q.Len(); got != n {
		t.Fatalf("Len = %d, want %d", got, n)
	}

	results := make(chan Item, n)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n/8; i++ {
				it, err := q.Pop(context.Background())
				if err != nil {
					t.Errorf("Pop: %v", err)
					return
				}
				results <- it
			}
		}()
	}
	wg.Wait()
	close(results)

	seen := make(map[uint64]int, n)
	for it := range results {
		seen[it.ID]++
	}
	if len(seen) != n {
		t.Fatalf("got %d distinct ids, want %d", len(seen), n)
	}
	for id := uint64(1); id <= n; id++ {
		if seen[id] != 1 {
			t.Fatalf("id %d delivered %d times, want exactly 1", id, seen[id])
		}
	}
}

func TestCloseDrainAndSnapshot(t *testing.T) {
	q := New()
	id1, _ := q.Enqueue(5, "low")
	if _, err := q.Enqueue(1, "high"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.Enqueue(1, "high-2"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	snap := q.Snapshot()
	if snap.Closed || snap.Waiting != 0 || snap.NextID != 4 {
		t.Fatalf("Snapshot meta = %+v, want Closed=false Waiting=0 NextID=4", snap)
	}
	wantOrder := []string{"high", "high-2", "low"}
	if len(snap.Items) != len(wantOrder) {
		t.Fatalf("Snapshot has %d items, want %d", len(snap.Items), len(wantOrder))
	}
	for i, v := range wantOrder {
		if snap.Items[i].Value != v {
			t.Fatalf("Snapshot.Items[%d] = %q, want %q", i, snap.Items[i].Value, v)
		}
	}
	// The snapshot must be an independent copy.
	snap.Items[0].Value = "mutated"
	if q.Snapshot().Items[0].Value != "high" {
		t.Fatal("Snapshot shares state with the queue")
	}

	q.Close()
	q.Close() // idempotent
	if _, err := q.Enqueue(9, "nope"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after Close = %v, want ErrClosed", err)
	}
	if err := q.Cancel(id1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Cancel after Close = %v, want ErrClosed", err)
	}
	if !q.Snapshot().Closed {
		t.Fatal("Snapshot.Closed = false after Close")
	}
	// Pending items still drain in priority order.
	for _, v := range wantOrder {
		popExpect(t, q, v)
	}
	if _, err := q.Pop(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pop on drained closed queue = %v, want ErrClosed", err)
	}

	// Close wakes Pop calls blocked on an empty queue.
	q2 := New()
	res := make(chan error, 1)
	go func() {
		_, err := q2.Pop(context.Background())
		res <- err
	}()
	waitForWaiting(t, q2, 1)
	q2.Close()
	select {
	case err := <-res:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("woken Pop error = %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not wake blocked Pop")
	}
}
