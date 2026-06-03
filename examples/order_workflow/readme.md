# Order Processing Workflow

A comprehensive example that demonstrates all major gabsurd patterns in a single realistic order-processing pipeline.

## What It Shows

| Pattern | Where in `src/order_workflow.gleam` |
|---------|------|
| **Idempotent steps** | `context.step(ctx, "charge", ..., fn() { ... })` — skips on retry |
| **Passing data between steps** | Step 1 saves `charge_id` to checkpoint; step 2 reads it back |
| **Continuation-based decoders** | `charge_id_decoder()` uses `use id <- decode.field(...)` syntax |
| **Event-driven coordination** | `context.await_event(ctx, ...)` → suspends until webhook fires |
| **WorkflowResult type** | `Done` / `WaitEvent` — clean mapping to `Complete` / `Suspend` |
| **Idempotent task spawning** | `task.with_idempotency_key("order-" <> id)` prevents duplicates |
| **Worker lifecycle** | `worker.start(config)` → `started.data` → `worker.stop(w)` |
| **on_error hook** | Logs task name and reason JSON when the handler returns `Fail` |

## Workflow Steps

```
spawn task → charge card → capture charge → reserve inventory
           → await payment webhook → send confirmation email
```

If the worker crashes at any point, the task is retried. Completed steps are skipped via checkpoints — only the remaining work runs.

### API patterns used

```gleam
// Connecting — client.start returns actor.StartResult(Db)
let assert Ok(started) = client.start(db_url)
let db = started.data

// Spawning — with idempotency key to prevent duplicates
let assert Ok(info) =
  task.spawn(db, "orders", "process_order", params,
    task.new_options()
    |> task.with_max_attempts(5)
    |> task.with_idempotency_key("order-" <> order_id),
  )

// Idempotent step — context.step(ctx, name, decoder, fn() -> Json)
use _ <- result.try(context.step(ctx, "reserve", decode.success(Nil), fn() {
  reserve_inventory(params.order_id)
  json.null()
}))

// Step that returns data — decoder uses Gleam's continuation-based API
fn charge_id_decoder() -> decode.Decoder(String) {
  use charge_id <- decode.field("charge_id", decode.string)
  decode.success(charge_id)
}
use charge_id <- result.try(
  context.step(ctx, "charge", charge_id_decoder(), fn() {
    json.object([#("charge_id", json.string("ch_123"))])
  }),
)

// Event-driven — context.await_event returns Received or Suspended
case context.await_event(ctx, "payment_confirmed_" <> order_id, 3600) {
  Ok(context.Received(_payload)) -> Ok(Done)
  Ok(context.Suspended) -> Ok(WaitEvent)
  Error(e) -> Error(e)
}

// Emitting events (from your webhook handler, separate process)
event.emit(db, "orders", "payment_confirmed_" <> order_id, json.object([...]))

// Worker setup — start returns actor.StartResult(Worker)
let config = worker.new(db, "orders", [handler])
  |> worker.with_poll_interval(500)
let assert Ok(started) = worker.start(config)
let w = started.data
// ... later:
worker.stop(w)
```

## Running

Requires a running PostgreSQL with the Absurd schema loaded (same DB as the main project):

```bash
# From the repo root — start the database and load the schema
mise run db
mise run db-reset

# Then run the example
cd examples/order_workflow
gleam run -m order_workflow
```

Or use the mise tasks from the repo root:

```bash
mise run example-build
mise run example-format
mise run example-check
mise run example-run
```

## Expected Output

```
=== gabsurd order_workflow example ===

[main] Connecting to database...
[main] Connected.
[main] Queue 'orders' ensured.
[main] Spawned task: attempt=1
[main] Worker started.
[main] Waiting for steps 1-3 (charge, capture, reserve)...

[charge_card] Charging $29.99 for order ord_001
[charge_card] Created charge: ch_ord_001
[capture_charge] Capturing charge: ch_ord_001
[reserve_inventory] Reserving stock for order: ord_001

[main] Emitting payment_confirmed event (simulating webhook)...
[main] Waiting for task to complete (steps 4-5)...
[send_confirmation] Sending email to alice@example.com for order ord_001

[main] Task result: state=completed result={"status": "completed"}

=== Done! ===
```

## Using in Your Project

1. Copy `src/order_workflow.gleam` into your project's `src/` directory.
2. Add `gabsurd` to your `gleam.toml` dependencies.
3. Replace the stub functions (`charge_card`, `capture_charge`, etc.) with your real business logic.
4. Wire up task spawning in your API handler and emit events from your webhook endpoint.
