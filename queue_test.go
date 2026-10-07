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

// waitFor spins until cond holds, using a deadline only as a failure guard
// (never as a synchronization sleep).
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func mustEnqueue(t *testing.T, q *Queue, priority int, value string) uint64 {
	t.Helper()
	id, err := q.Enqueue(priority, value)
	if err != nil {
		t.Fatalf("Enqueue(%d, %q): %v", priority, value, err)
	}
	return id
}

func mustPop(t *testing.T, q *Queue) Item {
	t.Helper()
	it, err := q.Pop(context.Background())
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	return it
}

func TestPopPriorityOrder(t *testing.T) {
	q := New()
	defer q.Close()

	type in struct {
		p int
		v string
	}
	inputs := []in{{5, "p5"}, {1, "p1-a"}, {3, "p3"}, {1, "p1-b"}, {2, "p2"}}
	for i, in := range inputs {
		id := mustEnqueue(t, q, in.p, in.v)
		if want := uint64(i + 1); id != want {
			t.Fatalf("Enqueue id = %d, want %d (IDs must increase from 1)", id, want)
		}
	}
	if got := q.Len(); got != len(inputs) {
		t.Fatalf("Len = %d, want %d", got, len(inputs))
	}

	wantValues := []string{"p1-a", "p1-b", "p2", "p3", "p5"}
	wantIDs := []uint64{2, 4, 5, 3, 1}
	for i := range wantValues {
		it := mustPop(t, q)
		if it.Value != wantValues[i] || it.ID != wantIDs[i] {
			t.Fatalf("Pop #%d = {ID:%d Value:%q}, want {ID:%d Value:%q}",
				i, it.ID, it.Value, wantIDs[i], wantValues[i])
		}
	}
	if got := q.Len(); got != 0 {
		t.Fatalf("Len after drain = %d, want 0", got)
	}
}

func TestSamePriorityFIFO(t *testing.T) {
	q := New()
	defer q.Close()

	const n = 6
	for i := 0; i < n; i++ {
		mustEnqueue(t, q, 7, fmt.Sprintf("v%d", i))
	}
	for i := 0; i < n; i++ {
		it := mustPop(t, q)
		if it.ID != uint64(i+1) || it.Value != fmt.Sprintf("v%d", i) {
			t.Fatalf("Pop #%d = {ID:%d Value:%q}, want FIFO order {ID:%d Value:%q}",
				i, it.ID, it.Value, i+1, fmt.Sprintf("v%d", i))
		}
	}
}

func TestCancelKeepsHeapConsistent(t *testing.T) {
	q := New()
	defer q.Close()

	priorities := []int{1, 2, 2, 3, 1, 2, 3, 1, 2} // ids 1..9
	for i, p := range priorities {
		mustEnqueue(t, q, p, fmt.Sprintf("v%d", i+1))
	}
	for _, id := range []uint64{2, 5, 9} { // remove items from the middle of the heap
		if err := q.Cancel(id); err != nil {
			t.Fatalf("Cancel(%d): %v", id, err)
		}
	}
	if got := q.Len(); got != 6 {
		t.Fatalf("Len = %d, want 6", got)
	}

	// The id index and the heap array must agree with each other, and the
	// heap invariant must hold after the removals.
	q.mu.Lock()
	for i, e := range q.h {
		if e.index != i {
			t.Fatalf("entry id=%d has index %d, sits at %d", e.item.ID, e.index, i)
		}
		if q.pending[e.item.ID] != e {
			t.Fatalf("pending index does not point at heap entry for id=%d", e.item.ID)
		}
		if i > 0 && q.h.Less(i, (i-1)/2) {
			t.Fatalf("heap invariant violated at index %d", i)
		}
	}
	q.mu.Unlock()

	wantIDs := []uint64{1, 8, 3, 6, 4, 7} // priority order, FIFO within ties
	for _, want := range wantIDs {
		if it := mustPop(t, q); it.ID != want {
			t.Fatalf("Pop ID = %d, want %d", it.ID, want)
		}
	}
}

func TestCancelInvalidAndNonPendingIDs(t *testing.T) {
	q := New()

	if err := q.Cancel(0); !errors.Is(err, ErrNotPending) {
		t.Fatalf("Cancel(0) = %v, want ErrNotPending", err)
	}
	if err := q.Cancel(42); !errors.Is(err, ErrNotPending) {
		t.Fatalf("Cancel(unknown) = %v, want ErrNotPending", err)
	}

	popped := mustEnqueue(t, q, 1, "popped")
	canceled := mustEnqueue(t, q, 2, "canceled")
	kept := mustEnqueue(t, q, 3, "kept")

	if it := mustPop(t, q); it.ID != popped {
		t.Fatalf("Pop ID = %d, want %d", it.ID, popped)
	}
	if err := q.Cancel(popped); !errors.Is(err, ErrNotPending) {
		t.Fatalf("Cancel(popped) = %v, want ErrNotPending", err)
	}
	if err := q.Cancel(canceled); err != nil {
		t.Fatalf("Cancel(pending) = %v, want nil", err)
	}
	if err := q.Cancel(canceled); !errors.Is(err, ErrNotPending) {
		t.Fatalf("Cancel(canceled) = %v, want ErrNotPending", err)
	}

	q.Close()
	if err := q.Cancel(kept); !errors.Is(err, ErrClosed) {
		t.Fatalf("Cancel after Close = %v, want ErrClosed", err)
	}
}

func TestBlockingPopWokenByEnqueue(t *testing.T) {
	q := New()
	defer q.Close()

	type result struct {
		item Item
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		it, err := q.Pop(context.Background())
		resCh <- result{item: it, err: err}
	}()

	waitFor(t, "Pop to block", func() bool { return q.Snapshot().Waiting == 1 })

	id := mustEnqueue(t, q, 7, "wake-me")
	select {
	case r := <-resCh:
		if r.err != nil {
			t.Fatalf("blocked Pop returned error: %v", r.err)
		}
		if r.item.ID != id || r.item.Value != "wake-me" {
			t.Fatalf("blocked Pop got %+v, want id=%d value=%q", r.item, id, "wake-me")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Pop was not woken by Enqueue")
	}
	waitFor(t, "Waiting to drop to 0", func() bool { return q.Snapshot().Waiting == 0 })
}

func TestPopContextCancel(t *testing.T) {
	q := New()
	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := q.Pop(ctx)
		errCh <- err
	}()

	waitFor(t, "Pop to block", func() bool { return q.Snapshot().Waiting == 1 })
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Pop error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pop did not return after context cancel")
	}

	// The cancel affected only that Pop: the queue still works and nothing
	// is lost or duplicated.
	id := mustEnqueue(t, q, 1, "after-cancel")
	if it := mustPop(t, q); it.ID != id {
		t.Fatalf("Pop ID = %d, want %d", it.ID, id)
	}

	// An already-canceled context on an empty queue returns immediately.
	if _, err := q.Pop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Pop with canceled ctx = %v, want context.Canceled", err)
	}
}

func TestConcurrentPopNoDuplicates(t *testing.T) {
	q := New()

	const n = 100
	for i := 1; i <= n; i++ {
		mustEnqueue(t, q, i*7%5, fmt.Sprintf("v%d", i)) // ids are 1..n
	}
	q.Close() // pops drain the queue, then see ErrClosed and stop

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		got = make(map[uint64]string, n)
	)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				it, err := q.Pop(context.Background())
				if errors.Is(err, ErrClosed) {
					return
				}
				if err != nil {
					t.Errorf("Pop: %v", err)
					return
				}
				mu.Lock()
				if _, dup := got[it.ID]; dup {
					t.Errorf("id %d delivered more than once", it.ID)
				}
				got[it.ID] = it.Value
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(got) != n {
		t.Fatalf("delivered %d distinct items, want %d", len(got), n)
	}
	for id := uint64(1); id <= n; id++ {
		if v, ok := got[id]; !ok || v != fmt.Sprintf("v%d", id) {
			t.Fatalf("id %d delivered as %q (ok=%v), want %q", id, v, ok, fmt.Sprintf("v%d", id))
		}
	}
}

func TestCloseDrainAndSnapshot(t *testing.T) {
	q := New()

	idA := mustEnqueue(t, q, 2, "a") // 1
	idB := mustEnqueue(t, q, 1, "b") // 2
	idC := mustEnqueue(t, q, 3, "c") // 3
	idD := mustEnqueue(t, q, 1, "d") // 4
	if err := q.Cancel(idC); err != nil {
		t.Fatalf("Cancel(%d): %v", idC, err)
	}

	snap := q.Snapshot()
	if snap.NextID != 5 || snap.Waiting != 0 || snap.Closed {
		t.Fatalf("Snapshot meta = {NextID:%d Waiting:%d Closed:%v}, want {5 0 false}",
			snap.NextID, snap.Waiting, snap.Closed)
	}
	wantIDs := []uint64{idB, idD, idA}
	if len(snap.Items) != len(wantIDs) {
		t.Fatalf("Snapshot has %d items, want %d", len(snap.Items), len(wantIDs))
	}
	for i, want := range wantIDs {
		if snap.Items[i].ID != want {
			t.Fatalf("Snapshot.Items[%d].ID = %d, want %d", i, snap.Items[i].ID, want)
		}
	}

	// The snapshot is an independent copy: mutating it must not affect the queue.
	snap.Items[0].Value = "mutated"
	if again := q.Snapshot(); again.Items[0].Value != "b" {
		t.Fatalf("Snapshot shares state with the queue: first value = %q", again.Items[0].Value)
	}

	q.Close()
	q.Close() // idempotent

	if _, err := q.Enqueue(1, "nope"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after Close = %v, want ErrClosed", err)
	}
	if err := q.Cancel(idA); !errors.Is(err, ErrClosed) {
		t.Fatalf("Cancel after Close = %v, want ErrClosed", err)
	}

	// Items pending at Close still drain in Pop order.
	for _, want := range []uint64{idB, idD, idA} {
		if it := mustPop(t, q); it.ID != want {
			t.Fatalf("drain Pop ID = %d, want %d", it.ID, want)
		}
	}
	if _, err := q.Pop(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pop on drained closed queue = %v, want ErrClosed", err)
	}
	if got := q.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
	if snap := q.Snapshot(); !snap.Closed || snap.Waiting != 0 || len(snap.Items) != 0 {
		t.Fatalf("final Snapshot = %+v, want empty and closed", snap)
	}

	// A Pop blocked on an empty queue is woken by Close and sees ErrClosed.
	q2 := New()
	resCh := make(chan error, 1)
	go func() {
		_, err := q2.Pop(context.Background())
		resCh <- err
	}()
	waitFor(t, "Pop to block", func() bool { return q2.Snapshot().Waiting == 1 })
	q2.Close()
	select {
	case err := <-resCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked Pop woken by Close = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Pop was not woken by Close")
	}
}
