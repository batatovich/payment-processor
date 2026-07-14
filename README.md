# payment-processor

A small HTTP payment/ledger service in Rust. It registers clients and tracks their running balances in memory, periodically flushing them to disk.
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
| `GET  /`                      | List available endpoints                 |
| `POST /new_client`            | Register a new client, returns client ID |
| `POST /new_credit_transaction`| Credit a client's balance                |
| `POST /new_debit_transaction` | Debit a client's balance (may go negative)|
| `POST /store_balances`        | Flush dirty balances to disk             |
| `GET  /get_balance`           | Read a client's document number & balance |

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

## Core architectural decisions

- **In-memory first, disk second.** Balances live in `Cache` and are mutated per request; durability comes from explicit `store_balances` flushes rather than a write per transaction. This keeps transaction handling fast at the cost of requiring periodic flushes.

- **Balances reset to zero on boot.** On startup only client *metadata* is rehydrated; balances always start at zero (`bootstrap.rs`).

- **Crash-safe writes.** Balance files are written to a `.tmp` file, `fsync`ed, then atomically renamed into place, so a crash never leaves a half-written file. An orphan `.tmp` found at boot aborts startup. Client metadata is an append-only JSON-lines ledger.

- **Monotonic nonce sequence.** Each flush produces a `ddmmyyyy_<nonce>.dat` file. At boot the files must form an unbroken `1..=N` sequence; gaps, duplicates, or orphan temp files fail the boot, giving a simple integrity check over the persisted history.

- **Dirty tracking + non-destructive flush.** Only clients with unpersisted changes are flushed. After writing, persisted amounts are *subtracted* (not hard-set to zero) so a transaction landing mid-flush is preserved and the client stays dirty for the next round.

- **Concurrency.** An `RwLock` guards the client map (read for balance ops, write only for registration); each balance sits behind its own `Mutex`; a dedicated `persistence_lock` serializes flushes; and `pending_registrations` guards against two concurrent sign-ups racing on the same document number before either is persisted.

- **Persist-then-publish client registration.** A new client is written to storage *before* being added to the cache. If the write fails, the document reservation is released and the client is never made visible, so the request can be safely retried.
