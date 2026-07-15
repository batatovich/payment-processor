# payment-processor

A little HTTP payment/ledger service written in Rust. It signs up clients, keeps their running balances in memory and saves them to a dedicated file if needed.

## Stack

- **[actix-web](https://actix.rs/)** — HTTP server
- **[tokio](https://tokio.rs/)** — async runtime and async file I/O
- **[rust_decimal](https://docs.rs/rust_decimal/)** — exact decimal arithmetic for money (no float rounding)
- **[uuid](https://docs.rs/uuid/) (v7)** — time-ordered client IDs
- **serde / serde_json** — (de)serialization
- **thiserror** — error type

## Running

```bash
cargo run
```

Server binds to `127.0.0.1:8080` (see `src/constants.rs`). State is persisted under `./data/`.

## API

| Method & Path                 | Description                              |
| ----------------------------- | ---------------------------------------- |
| `POST /new_client`            | Register a new client, returns client ID |
| `POST /new_credit_transaction`| Credit a client's balance                |
| `POST /new_debit_transaction` | Debit a client's balance (may go negative)|
| `POST /store_balances`        | Flush dirty balances to disk             |
| `GET  /get_balance`           | Read a client's document number & current balance |

`GET /get_balance` gives you the **current in-memory balance**, not the client's net/lifetime balance. Because a flush zeroes out what it persists, the number you get back is only what has piled up since the last `store_balances`.

## Layout

```
src/
  main.rs         Server setup and route registration
  bootstrap.rs    Startup: init data dir, recover nonce, hydrate clients cache
  cache.rs        In-memory application state
  storage.rs      File read/write functions
  api/
    handlers.rs   HTTP endpoint handlers
    dto.rs        Request/response bodies
  model.rs        Custom struct/types/enums (Client, Country, TransactionDirection)
  error.rs        AppError + HTTP status mapping
  constants.rs    Config constants (host, port, paths)
```
## Design notes

**In-memory first, disk second.** Balances live in `Cache` and update on every request. Durability comes from explicit `store_balances` calls rather than writing to disk on every transaction.

**Balances start at zero on boot.** Only client metadata is rehydrated at startup (`bootstrap.rs`) — balances aren't restored from previous runs, they always begin at zero.

**Crash-safe writes.** Balance files are written to a `.tmp` path, fsynced, then renamed into place, so a crash mid-write can't leave a corrupt file behind. If an orphan `.tmp` file is found at boot, startup fails rather than guessing what happened. Client metadata, by contrast, is a plain append-only JSON-lines file.

**Nonce sequence has to be unbroken.** Each flush produces a `ddmmyyyy_<nonce>.dat` file. At boot, the existing files must form a complete `1..=N` sequence with no gaps or duplicates — if they don't (or there's an orphan temp file), startup refuses to continue. It's a simple integrity check across the whole history.

**Dirty tracking, non-destructive flush.** Only clients with unpersisted changes get written. After a flush, the persisted amount is subtracted from the in-memory balance rather than zeroing it outright — so a transaction that arrives mid-flush isn't lost, it just leaves the client dirty for next time.

**Concurrency.** The client map sits behind an `RwLock` (read for balance operations, write only for registering a new client), each client's balance has its own `Mutex`, a `persistence_lock` serializes `store_balances` calls, and `pending_registrations` prevents two concurrent sign-ups from racing on the same document number before either is persisted.

**Registration writes to disk before it's visible in memory.** A new client is persisted first; only after that succeeds does it get added to the cache. If the write fails, the document reservation is released and the client never becomes visible, so the request can just be retried.