// Command pqdemo is a small end-to-end demo of the cancelablepriorityqueue
// library: mixed priorities, FIFO within one priority, Cancel, a blocked
// Pop woken by Enqueue, and draining after Close.
package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	cpq "cancelablepriorityqueue"
)

func main() {
	q := cpq.New()
	ctx := context.Background()

	enqueue := func(priority int, value string) uint64 {
		id, err := q.Enqueue(priority, value)
		if err != nil {
			panic(err)
		}
		fmt.Printf("  Enqueue(priority=%d, value=%q) -> id=%d\n", priority, value, id)
		return id
	}

	fmt.Println("== 1. Enqueue items with different priorities (lower pops first) ==")
	enqueue(3, "write weekly report")       // id 1
	enqueue(1, "fix production bug")        // id 2
	enqueue(1, "review pull request")       // id 3: same priority as id 2, FIFO after it
	enqueue(5, "water the plants")          // id 4
	emailID := enqueue(2, "reply to email") // id 5

	fmt.Println("\n== 2. Snapshot: pending items in exact Pop order ==")
	snap := q.Snapshot()
	fmt.Printf("  Len=%d NextID=%d Waiting=%d Closed=%v\n", q.Len(), snap.NextID, snap.Waiting, snap.Closed)
	for _, it := range snap.Items {
		fmt.Printf("  id=%d priority=%d value=%q\n", it.ID, it.Priority, it.Value)
	}

	fmt.Println("\n== 3. Cancel the middle item ==")
	if err := q.Cancel(emailID); err != nil {
		panic(err)
	}
	fmt.Printf("  Cancel(id=%d) ok, Len=%d\n", emailID, q.Len())

	fmt.Println("\n== 4. Drain: priority order, FIFO within the same priority ==")
	for q.Len() > 0 {
		it, err := q.Pop(ctx)
		if err != nil {
			panic(err)
		}
		fmt.Printf("  Pop -> id=%d priority=%d value=%q\n", it.ID, it.Priority, it.Value)
	}

	fmt.Println("\n== 5. A blocked Pop is woken by a later Enqueue ==")
	type popResult struct {
		item cpq.Item
		err  error
	}
	resCh := make(chan popResult, 1)
	go func() {
		it, err := q.Pop(ctx)
		resCh <- popResult{item: it, err: err}
	}()
	for q.Snapshot().Waiting == 0 { // wait until the Pop is actually blocked
		runtime.Gosched()
	}
	fmt.Println("  Pop is blocked on the empty queue; enqueuing \"hotfix\"...")
	hotID, err := q.Enqueue(1, "hotfix")
	if err != nil {
		panic(err)
	}
	res := <-resCh
	fmt.Printf("  blocked Pop woke up -> id=%d value=%q (enqueued id=%d, err=%v)\n",
		res.item.ID, res.item.Value, hotID, res.err)

	fmt.Println("\n== 6. Close: reject new work, drain the rest, then report closed ==")
	lastA, err := q.Enqueue(2, "last task A")
	if err != nil {
		panic(err)
	}
	lastB, err := q.Enqueue(1, "last task B")
	if err != nil {
		panic(err)
	}
	fmt.Printf("  queued id=%d and id=%d, then Close()\n", lastA, lastB)
	q.Close()
	if _, err := q.Enqueue(1, "too late"); err != nil {
		fmt.Printf("  Enqueue after Close -> %v\n", err)
	}
	if err := q.Cancel(lastA); err != nil {
		fmt.Printf("  Cancel(id=%d) after Close -> %v\n", lastA, err)
	}
	for {
		it, err := q.Pop(ctx)
		if errors.Is(err, cpq.ErrClosed) {
			fmt.Println("  Pop on drained closed queue -> ErrClosed; demo done")
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Printf("  Pop -> id=%d priority=%d value=%q\n", it.ID, it.Priority, it.Value)
	}
}
