// Command pqdemo demonstrates the cancelablepriorityqueue library:
// mixed priorities, same-priority FIFO, Cancel, a blocking Pop woken by
// Enqueue, and draining after Close.
package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	cpq "cancelablepriorityqueue"
)

func mustEnqueue(q *cpq.Queue, priority int, value string) uint64 {
	id, err := q.Enqueue(priority, value)
	if err != nil {
		log.Fatalf("Enqueue(%d, %q): %v", priority, value, err)
	}
	fmt.Printf("  enqueued id=%d priority=%d value=%q\n", id, priority, value)
	return id
}

func mustPop(q *cpq.Queue, ctx context.Context) cpq.Item {
	it, err := q.Pop(ctx)
	if err != nil {
		log.Fatalf("Pop: %v", err)
	}
	return it
}

func main() {
	q := cpq.New()

	fmt.Println("== 1. Enqueue items with different priorities ==")
	mustEnqueue(q, 3, "send-newsletter")
	mustEnqueue(q, 1, "critical-alert")
	mustEnqueue(q, 2, "daily-report")
	mustEnqueue(q, 1, "sms-alert") // same priority as id=2, must FIFO after it
	mustEnqueue(q, 3, "sync-email")

	fmt.Println("== 2. Cancel a pending middle item (id=3, daily-report) ==")
	if err := q.Cancel(3); err != nil {
		log.Fatalf("Cancel: %v", err)
	}
	fmt.Println("  canceled id=3")

	fmt.Println("== 3. Pop drains by priority, FIFO within the same priority ==")
	for q.Len() > 0 {
		it := mustPop(q, context.Background())
		fmt.Printf("  popped id=%d priority=%d value=%q\n", it.ID, it.Priority, it.Value)
	}

	fmt.Println("== 4. A blocked Pop is woken by a later Enqueue ==")
	type result struct {
		it  cpq.Item
		err error
	}
	res := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		it, err := q.Pop(ctx)
		res <- result{it, err}
	}()
	for q.Snapshot().Waiting == 0 { // wait until the Pop above is blocked
		runtime.Gosched()
	}
	fmt.Println("  Pop is blocked on the empty queue; enqueuing one item")
	mustEnqueue(q, 0, "wake-up-call")
	r := <-res
	if r.err != nil {
		log.Fatalf("blocked Pop: %v", r.err)
	}
	fmt.Printf("  blocked Pop woke with id=%d value=%q\n", r.it.ID, r.it.Value)

	fmt.Println("== 5. Close rejects new work but still drains the queue ==")
	mustEnqueue(q, 2, "queued-before-close")
	mustEnqueue(q, 1, "also-before-close")
	q.Close()
	if _, err := q.Enqueue(0, "too-late"); err != nil {
		fmt.Printf("  Enqueue after Close: %v\n", err)
	}
	if err := q.Cancel(1); err != nil {
		fmt.Printf("  Cancel after Close: %v\n", err)
	}
	for {
		it, err := q.Pop(context.Background())
		if err != nil {
			fmt.Printf("  queue drained, Pop: %v\n", err)
			break
		}
		fmt.Printf("  drained id=%d priority=%d value=%q\n", it.ID, it.Priority, it.Value)
	}
	fmt.Println("demo done")
}
