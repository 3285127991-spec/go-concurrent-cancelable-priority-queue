# cancelablepriorityqueue

一个并发安全、可取消的 Go 优先级队列库，附带简洁 CLI 演示与自动化测试。
零第三方依赖，无内部后台 goroutine。

## 特性

- `Enqueue(priority, value)`：分配从 1 开始严格递增、永不重复的 `uint64` id。
- `priority` 为 `int`，数值越小优先级越高；同优先级严格按入队顺序 FIFO。
- `Pop(ctx)`：原子取出最高优先级（同级最早）的待处理项；队列为空且未关闭时阻塞等待；
  `ctx` 取消只影响当前这次 `Pop`，不会丢元素或重复交付。
- `Cancel(id)`：O(log n) 移除仍在队列中的项（堆 + id 索引，不做线性扫描）；
  `id=0` 返回 `ErrInvalidID`，未知或已非 pending 的 id 返回 `ErrNotPending`。
- `Close()`：拒绝新的 `Enqueue`/`Cancel`，唤醒所有阻塞中的 `Pop`；
  关闭前已入队的项仍按优先级排空，耗尽后 `Pop` 返回 `ErrClosed`。
- `Snapshot()`：返回按实际 Pop 顺序排列的独立副本，以及 `NextID`、`Waiting`、`Closed`。
- `Len()`：当前 pending 项数量。

## 使用

```go
q := cancelablepriorityqueue.New()

id, _ := q.Enqueue(1, "critical-alert")

it, err := q.Pop(context.Background()) // 阻塞直到有元素或关闭

_ = q.Cancel(id) // 仅当 id 仍 pending 时成功

snap := q.Snapshot() // snap.Items 按 Pop 顺序排列

q.Close()
```

## 运行 CLI 演示

```sh
go run ./cmd/pqdemo
```

演示：不同优先级入队、同优先级 FIFO、Cancel 中间项、阻塞 Pop 被 Enqueue 唤醒、Close 后排空。

## 运行测试

```sh
go test -race ./...
```

覆盖：优先级顺序、同优先级稳定 FIFO、Cancel 后堆结构正确、非法/非 pending id、
阻塞 Pop、context 取消（含取消与入队竞争）、并发 Pop 不重复、Close 排空与 Snapshot。
