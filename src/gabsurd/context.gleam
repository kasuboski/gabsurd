//// Execution context for a running task.
////
//// Constructed by the worker and passed to your handler. Encapsulates
//// the database connection, queue, claim details, and claim timeout so
//// you don't have to pass them around. Provides high-level operations
//// for idempotent steps, checkpoints, heartbeats, and event coordination.
////
//// ## Example
////
//// ```gleam
//// let handler = Handler(
////   task_name: "process_order",
////   execute: fn(ctx) {
////     case order_workflow(ctx) {
////       Ok(Nil) -> Complete(json.object([#("status", json.string("done"))]))
////       Error(e) -> Fail(encode_error(e))
////     }
////   },
////   on_error: option.None,
//// )
////
//// fn order_workflow(ctx) -> Result(Nil, GabsurdError) {
////   use _ <- result.try(ctx.step("charge", decode.success(Nil), fn() {
////     charge_card(decode_params(ctx.params(ctx)))
////     json.null()
////   }))
////   use _ <- result.try(ctx.step("reserve", decode.success(Nil), fn() {
////     reserve_inventory(decode_params(ctx.params(ctx)))
////     json.null()
////   }))
////   Ok(Nil)
//// }
//// ```

import gleam/dynamic/decode
import gleam/json
import gleam/option
import gabsurd/checkpoint
import gabsurd/client.{type Db, type GabsurdError}
import gabsurd/event
import gabsurd/task.{type Claim}

// ============================================================================
// Public Types
// ============================================================================

/// Execution context for a running task. Constructed by the worker,
/// passed to your handler.
pub type Context {
  Context(
    db: Db,
    queue_name: String,
    claim: Claim,
    claim_timeout: Int,
  )
}

/// Result of `await_event`. The event was either received (task continues)
/// or the task is now sleeping (return `Suspend` from your handler).
pub type EventResult {
  Received(String)
  Suspended
}

// ============================================================================
// Accessors
// ============================================================================

/// The task's unique identifier.
pub fn task_id(ctx: Context) -> BitArray {
  ctx.claim.task_id
}

/// The current run's unique identifier.
pub fn run_id(ctx: Context) -> BitArray {
  ctx.claim.run_id
}

/// The task parameters as a raw JSON string.
pub fn params(ctx: Context) -> String {
  ctx.claim.params
}

/// The task name.
pub fn task_name(ctx: Context) -> String {
  ctx.claim.task_name
}

/// The current attempt number (1-based).
pub fn attempt(ctx: Context) -> Int {
  ctx.claim.attempt
}

/// The claim timeout in seconds.
pub fn claim_timeout(ctx: Context) -> Int {
  ctx.claim_timeout
}

// ============================================================================
// Idempotent Steps
// ============================================================================

/// Run an idempotent step identified by name.
///
/// If the checkpoint already exists (from a previous attempt), the stored
/// value is decoded with `decoder` and returned without re-running `run`.
/// If not, `run` is executed, the result is persisted as a checkpoint, and
/// the claim lease is extended by `claim_timeout` seconds.
///
/// The `decoder` parameter is required because Gleam's `json.Json` type is
/// write-only — values loaded from the database must be parsed with an
/// explicit decoder. For steps that don't need a return value, use
/// `decode.success(Nil)`.
///
/// ## Example
///
/// ```gleam
/// // Step that returns a value:
/// use charge_id <- result.try(
///   ctx.step("charge", decode.field("charge_id", decode.string), fn() {
///     let result = charge_card(...)
///     json.object([#("charge_id", json.string(result.id))])
///   }),
/// )
///
/// // Step that doesn't return a value:
/// use _ <- result.try(ctx.step("notify", decode.success(Nil), fn() {
///   send_email(...)
///   json.null()
/// }))
/// ```
pub fn step(
  ctx: Context,
  name: String,
  decoder: decode.Decoder(a),
  run: fn() -> json.Json,
) -> Result(a, GabsurdError) {
  case checkpoint.get(ctx.db, ctx.queue_name, ctx.claim.task_id, name, False) {
    Ok(option.Some(cp)) -> {
      // Step already done — decode the stored value
      let assert Ok(value) = json.parse(cp.state, decoder)
      Ok(value)
    }
    Ok(option.None) -> {
      // Step not done — run it, persist, extend lease
      let json_value = run()
      let json_string = json.to_string(json_value)
      case
        checkpoint.set(
          ctx.db,
          ctx.queue_name,
          ctx.claim.task_id,
          name,
          json_value,
          ctx.claim.run_id,
          ctx.claim_timeout,
        )
      {
        Ok(Nil) -> {
          // Decode the persisted value through the decoder for consistency
          let assert Ok(value) = json.parse(json_string, decoder)
          Ok(value)
        }
        Error(e) -> Error(e)
      }
    }
    Error(e) -> Error(e)
  }
}

// ============================================================================
// Low-level Checkpoint Access
// ============================================================================

/// Get a checkpoint's raw JSON state string.
/// Returns `Ok(Some(json_string))` if found, `Ok(None)` if not found.
pub fn get_checkpoint(
  ctx: Context,
  name: String,
) -> Result(option.Option(String), GabsurdError) {
  case checkpoint.get(ctx.db, ctx.queue_name, ctx.claim.task_id, name, False) {
    Ok(option.Some(cp)) -> Ok(option.Some(cp.state))
    Ok(option.None) -> Ok(option.None)
    Error(e) -> Error(e)
  }
}

/// Set a checkpoint and extend the claim lease.
pub fn set_checkpoint(
  ctx: Context,
  name: String,
  state: json.Json,
) -> Result(Nil, GabsurdError) {
  checkpoint.set(
    ctx.db,
    ctx.queue_name,
    ctx.claim.task_id,
    name,
    state,
    ctx.claim.run_id,
    ctx.claim_timeout,
  )
}

// ============================================================================
// Lease Management
// ============================================================================

/// Extend the claim lease by `claim_timeout` seconds.
pub fn heartbeat(ctx: Context) -> Result(Nil, GabsurdError) {
  task.extend_claim(
    ctx.db,
    ctx.queue_name,
    ctx.claim.run_id,
    ctx.claim_timeout,
  )
}

// ============================================================================
// Event Coordination
// ============================================================================

/// Await an external event. If the event is already available, returns
/// `Received(payload)`. If not, the task is put to sleep and returns
/// `Suspended` — your handler should return `Suspend` in this case.
///
/// `timeout` is in seconds. Set to `0` for no timeout.
pub fn await_event(
  ctx: Context,
  event_name: String,
  timeout: Int,
) -> Result(EventResult, GabsurdError) {
  case
    event.await(
      ctx.db,
      ctx.queue_name,
      ctx.claim.task_id,
      ctx.claim.run_id,
      "$await:" <> event_name,
      event_name,
      timeout,
    )
  {
    Ok(result) -> {
      case result.should_suspend {
        True -> Ok(Suspended)
        False -> Ok(Received(result.payload))
      }
    }
    Error(e) -> Error(e)
  }
}
