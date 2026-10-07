# cancelablepriorityqueue

一个并发安全、可取消的优先级队列 Go 库，附简洁 CLI 演示与自动化测试。零第三方依赖，仅使用标准库，无内部后台 goroutine。

## 特性

- `Enqueue(priority, value)`：成功时分配从 1 开始严格递增、永不重复的 `uint64` id。
- `Pop(ctx)`：原子取出优先级最高（数值最小）的待处理项；同优先级严格 FIFO。空队列且未关闭时阻塞等待；多个并发 `Pop` 不会重复交付同一项。
- `Cancel(id)`：O(log n) 移除仍在队列中的项（堆 + id 索引，不做线性扫描）。id 为 0、未知或已非 pending 时返回 `ErrNotPending`。
- `Snapshot()`：返回当前 pending 项按实际 Pop 顺序排列的独立副本，以及 `NextID`、`Waiting`、`Closed`。
- `Len()`：当前 pending 项数量。
- `Close()`：拒绝新的 `Enqueue`/`Cancel`（返回 `ErrClosed`），唤醒所有阻塞的 `Pop`；剩余项仍可按序排空，耗尽后 `Pop` 返回 `ErrClosed`。幂等。
- 等待中的 `ctx` 取消只影响当前 `Pop`；取消与 `Enqueue` 并发时只会得到“取消”或“成功 Pop”之一，不丢元素、不重复交付。

## API

```go
q := cancelablepriorityqueue.New()
id, err := q.Enqueue(1, "task")   // priority 越小越先出队
item, err := q.Pop(ctx)           // item: {ID, Priority, Value}
err = q.Cancel(id)                // ErrNotPending / ErrClosed
snap := q.Snapshot()              // {Items, NextID, Waiting, Closed}
n := q.Len()
q.Close()
```

错误：`ErrClosed`（关闭后入队/取消，或关闭且排空后的 Pop）、`ErrNotPending`（取消无效 id）、`ctx.Err()`（Pop 等待时 context 取消）。

复杂度：`Enqueue`/`Pop`/`Cancel` 为 O(log n)，`Len` 为 O(1)，`Snapshot` 为 O(n log n)。

## 运行 CLI 演示

```sh
go run ./cmd/pqdemo
```

演示内容：不同优先级入队、同优先级 FIFO、Cancel 中间项、阻塞 Pop 被 Enqueue 唤醒、Close 后拒绝新操作并按序排空。

## 测试

```sh
go test -race ./...
```

8 个测试覆盖：优先级顺序、同优先级稳定 FIFO、Cancel 后堆结构一致性、非法/非 pending id、阻塞 Pop 唤醒、context 取消、并发 Pop 不重复、Close 排空与 Snapshot。

## 明确不做

动态优先级修改、容量限制、延迟任务、重试、TTL、持久化、网络、GUI、数据库。

