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

## The interesting bits (a.k.a. why it's built this way)

- **In-memory first, disk second.** Balances live in `Cache` and get bumped on every request; durability comes from explicit `store_balances` flushes instead of a write per transaction.

- **Balances reset to zero on boot.** On startup only client *metadata* gets rehydrated — balances always come back up at zero (`bootstrap.rs`).

- **Crash-safe writes.** Balance files get written to a `.tmp` file, `fsync`ed, then atomically renamed into place, so a crash can't leave a half-written file lying around. If an orphan `.tmp` shows up at boot, startup bails. Client metadata is a plain append-only JSON-lines ledger.

- **Monotonic nonce sequence.** Every flush spits out a `ddmmyyyy_<nonce>.dat` file. At boot those files have to form an unbroken `1..=N` sequence — any gaps, duplicates, or orphan temp files and boot refuses to start. Cheap little integrity check over the whole history.

- **Dirty tracking + non-destructive flush.** Only clients with unsaved changes get flushed. After writing, the persisted amount is *subtracted* rather than hard-set to zero, so a transaction that sneaks in mid-flush isn't lost and the client just stays dirty for the next round.

- **Concurrency.** An `RwLock` guards the client map (read for balance ops, write only for registration); each balance sits behind its own `Mutex`; a dedicated `persistence_lock` serializes flushes; and `pending_registrations` stops two concurrent sign-ups from racing on the same document number before either one is persisted.

- **Persist-then-publish registration.** A new client hits storage *before* it's added to the cache. If the write fails, the document reservation is released and the client never becomes visible — so you can just retry.
