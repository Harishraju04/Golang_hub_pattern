# WebSocket Hub Pattern in Go

A thread-safe pattern for managing multiple WebSocket connections and broadcasting messages.

---

## What Problem Does This Solve?

When multiple clients connect over WebSocket, you need a safe way to:
- Track all active connections
- Broadcast a message to all of them
- Remove a connection when it disconnects

Doing this with a raw `map` and scattered `mutex` calls leads to data races and deadlocks. The Hub pattern encapsulates all of that behind clean methods.

---

## Structure

```
main.go         → wires up HTTP server, creates Hub, starts Run()
hub.go          → Hub struct, Register, UnRegister, Broadcast, Run
handler.go      → wsHandler upgrades HTTP → WebSocket, spawns handleConnection
```

---

## The Hub

```go
type Hub struct {
    clients   map[*websocket.Conn]bool
    mutex     sync.RWMutex
    broadcast chan []byte
}
```

| Field | Purpose |
|---|---|
| `clients` | tracks all active connections |
| `mutex` | `RWMutex` — many readers, one writer |
| `broadcast` | channel that carries messages to be sent to all clients |

---

## Core Methods

### Register
```go
func (h *Hub) Register(conn *websocket.Conn) {
    h.mutex.Lock()
    defer h.mutex.Unlock()
    h.clients[conn] = true
}
```
Called when a new client connects. Acquires a write lock because it mutates the map.

### UnRegister
```go
func (h *Hub) UnRegister(conn *websocket.Conn) {
    h.mutex.Lock()
    defer h.mutex.Unlock()
    if _, ok := h.clients[conn]; ok {
        delete(h.clients, conn)
        conn.Close()
    }
}
```
Called when a client disconnects or write fails. Checks existence before deleting to avoid double-close panics.

### getSnapshot
```go
func (h *Hub) getSnapshot() []*websocket.Conn {
    h.mutex.RLock()
    defer h.mutex.RUnlock()
    snapshot := make([]*websocket.Conn, 0, len(h.clients))
    for client := range h.clients {
        snapshot = append(snapshot, client)
    }
    return snapshot
}
```
Copies map keys into a plain slice and releases the lock immediately.
This lets `Run()` do slow network writes without holding any lock.

> **Common mistake:** `make([]*websocket.Conn, len(h.clients))` pre-fills the slice with `nil` pointers — append then adds connections *after* the nils, causing a nil pointer panic on write. Always use `make(T, 0, cap)` when building a slice with append.

### Run
```go
func (h *Hub) Run() {
    for msg := range h.broadcast {
        clients := h.getSnapshot()
        for _, client := range clients {
            if err := client.WriteMessage(websocket.TextMessage, msg); err != nil {
                h.UnRegister(client)
            }
        }
    }
}
```
Single goroutine that owns all writes. Reads from `broadcast` channel forever.
Must be started with `go hub.Run()` in `main`.

---

## Flow

```
Client connects
    │
    ▼
wsHandler() — upgrades HTTP to WebSocket
    │
    ├── hub.Register(conn)         — adds to map (write lock)
    │
    └── go handleConnection()      — spawns goroutine per client

handleConnection() loop:
    │
    ├── conn.ReadMessage()         — waits for client to send
    │
    └── hub.Broadcast(msg)        — sends to broadcast channel
            │
            ▼
        hub.Run() picks it up
            │
            ├── getSnapshot()     — RLock, copy map, RUnlock
            │
            └── WriteMessage()    — sends to each client (no lock held)
                    │
                    └── on error → hub.UnRegister() — write lock, delete + close
```

---

## Why `sync.RWMutex` and Not `sync.Mutex`?

```
sync.Mutex    → one person at a time, always. reads block other reads.
sync.RWMutex  → many readers in parallel. writers are exclusive.
```

In a WebSocket hub, reads (getSnapshot) happen far more often than writes (register/unregister). `RWMutex` lets all reads run simultaneously, only blocking when someone is writing.

**Rule:** never hold a read lock (`RLock`) and then try to acquire a write lock (`Lock`) — Go's `RWMutex` has no upgrade path and will deadlock.

---

## Why the Snapshot Pattern?

Without a snapshot, you would hold the lock during network writes:

```
❌ Without snapshot:
RLock ──── slow network write ──── slow network write ──── RUnlock
           lock held the whole time, everyone else waits

✅ With snapshot:
RLock ── copy ── RUnlock
                  └── slow network writes (no lock held)
                       └── on error → fresh Lock() to delete, no conflict
```

---

## Key Gotchas

| Gotcha | What Happens | Fix |
|---|---|---|
| `make([]T, n)` instead of `make([]T, 0, n)` | slice pre-filled with nils, panic on write | always use `0, cap` when appending |
| `handleMessages` without a `for` loop | handles one message, goroutine dies | wrap in `for {}` |
| Not deferring `UnRegister` in `handleConnection` | connection leaks on disconnect | `defer hub.UnRegister(conn)` |
| Holding RLock and calling Lock | deadlock | release all locks before re-acquiring |
| Unbuffered broadcast channel | sender blocks if Run() is busy | `make(chan []byte, 256)` |

---

## Scaling Further

For high connection counts or high message frequency:

- **Reuse snapshot slice** — store it on Hub, reset with `h.snapshot = h.snapshot[:0]` each run to eliminate per-broadcast allocation
- **Buffered broadcast channel** — `make(chan []byte, 256)` prevents `handleConnection` from blocking when `Run()` is busy
- **`sync.Pool`** — pool snapshot slices to reduce GC pressure at very high throughput
- **Per-client send channel** — instead of writing directly in `Run()`, give each client its own buffered channel and a dedicated write goroutine; slow clients don't block the broadcast loop
