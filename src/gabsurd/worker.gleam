//// OTP Worker actor for the Absurd durable workflow system.
//// Polls a queue for tasks, dispatches to registered handlers,
//// and completes/fails tasks based on handler results.
////
//// ## Distributed systems behaviour
////
//// - **Claim extension**: The primary lease extension mechanism is
////   `checkpoint.set`, which passes `claim_timeout` as `extend_claim_by`
////   to `set_task_checkpoint_state` — every checkpoint write extends the
////   lease. For handlers doing a single long operation without checkpoints,
////   the claim timeout is the safety net: if the handler takes too long,
////   the claim expires and another worker picks up the task.
//// - **Error backoff**: On transient claim errors, backs off exponentially
////   up to `max_backoff` (default 60s), resets on success.
//// - **Unknown task deferral**: Tasks with no registered handler are deferred
////   (rescheduled with a delay) rather than failed. This supports rolling
////   deployments where a new task type may arrive before its handler is
////   deployed.
//// - **Terminal state tolerance**: complete/fail errors from already-
////   completed or already-failed runs are silently ignored (matching the
////   official Absurd SDK behaviour).

import gabsurd/client.{
  type Db, type GabsurdError, QueryError, UnexpectedRowCount, NotFound,
  ConnectionError,
}
import gabsurd/task.{type Claim}
import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/supervision
import gleam/string

// ============================================================================
// Public Types
// ============================================================================

/// A handler for a specific task type.
///
/// Define one of these for each kind of task your workers should process.
/// The worker dispatches claimed tasks to the handler matching their `task_name`.
///
/// ## Example
///
/// ```gleam
/// let email_handler = worker.Handler(
///   task_name: "send_email",
///   execute: fn(claim) {
///     // ... send email ...
///     Ok(json.object([#("sent", json.bool(True))]))
///   },
///   on_error: option.None,
/// )
/// ```
pub type Handler {
  Handler(
    /// The task_name this handler responds to.
    task_name: String,
    /// Execute the task. Return `Ok(json)` to complete or `Error(json)` to fail.
    execute: fn(Claim) -> Result(json.Json, json.Json),
    /// Optional hook called when execute returns Error.
    /// Receives the claim and the error json. Use for logging/metrics.
    on_error: option.Option(fn(Claim, json.Json) -> Nil),
  )
}

/// Worker configuration. Create with `new()` and customize with `with_*` functions.
pub type Config {
  Config(
    db: Db,
    queue_name: String,
    worker_id: String,
    poll_interval: Int,
    claim_timeout: Int,
    batch_size: Int,
    max_backoff: Int,
    handlers: List(Handler),
  )
}

/// A running worker actor handle.
pub type Worker {
  Worker(subject: process.Subject(Message))
}

/// Messages the worker actor accepts.
pub type Message {
  Poll
  Shutdown
}

// ============================================================================
// Internal State
// ============================================================================

type WorkerState {
  WorkerState(
    config: Config,
    handler_map: dict.Dict(String, Handler),
    subject: process.Subject(Message),
    consecutive_errors: Int,
  )
}

// ============================================================================
// Configuration
// ============================================================================

/// Create a new worker config with defaults.
pub fn new(db: Db, queue_name: String, handlers: List(Handler)) -> Config {
  Config(
    db: db,
    queue_name: queue_name,
    worker_id: "gabsurd_worker",
    poll_interval: 5000,
    claim_timeout: 30,
    batch_size: 1,
    max_backoff: 60000,
    handlers: handlers,
  )
}

/// Set the worker ID (used as `worker_id` in claim_task).
pub fn with_worker_id(config: Config, worker_id: String) -> Config {
  Config(..config, worker_id: worker_id)
}

/// Set the poll interval in milliseconds.
pub fn with_poll_interval(config: Config, interval_ms: Int) -> Config {
  Config(..config, poll_interval: interval_ms)
}

/// Set the claim timeout in seconds.
pub fn with_claim_timeout(config: Config, timeout_secs: Int) -> Config {
  Config(..config, claim_timeout: timeout_secs)
}

/// Set the batch size (tasks claimed per poll).
pub fn with_batch_size(config: Config, size: Int) -> Config {
  Config(..config, batch_size: size)
}

/// Set the maximum backoff in milliseconds for retrying after claim errors.
/// Default: 60000 (60 seconds).
pub fn with_max_backoff(config: Config, max_backoff_ms: Int) -> Config {
  Config(..config, max_backoff: max_backoff_ms)
}

// ============================================================================
// Lifecycle
// ============================================================================

/// Start a worker actor.
pub fn start(config: Config) -> actor.StartResult(Worker) {
  let handler_map =
    config.handlers
    |> list.map(fn(h) { #(h.task_name, h) })
    |> dict.from_list()

  let init = fn(subject: process.Subject(Message)) {
    let state =
      WorkerState(
        config: config,
        handler_map: handler_map,
        subject: subject,
        consecutive_errors: 0,
      )
    // Schedule first poll
    let _ = process.send_after(subject, config.poll_interval, Poll)
    actor.initialised(state)
    |> actor.returning(Worker(subject: subject))
    |> Ok
  }

  actor.new_with_initialiser(5000, init)
  |> actor.on_message(handle_message)
  |> actor.start()
}

/// Stop a worker gracefully.
pub fn stop(worker: Worker) -> Nil {
  process.send(worker.subject, Shutdown)
}

/// Create a child spec for adding to a static_supervisor.
pub fn child_spec(
  _name: String,
  config: Config,
) -> supervision.ChildSpecification(Worker) {
  supervision.worker(fn() { start(config) })
}

/// Create a list of child specs for a worker pool (N workers).
/// Each worker gets a unique worker_id incorporating a unique integer
/// to avoid collisions between pools.
pub fn pool_child_specs(
  name: String,
  config: Config,
  count: Int,
) -> List(supervision.ChildSpecification(Worker)) {
  let unique = client.unique_integer()
  list.index_fold(list.repeat(Nil, count), [], fn(acc, _, i) {
    let i = i + 1
    let wid =
      name
      <> "_" <> int.to_string(unique)
      <> "_" <> int.to_string(i)
    let config = Config(..config, worker_id: wid)
    [supervision.worker(fn() { start(config) }), ..acc]
  })
}

// ============================================================================
// Message Handler
// ============================================================================

fn handle_message(
  state: WorkerState,
  message: Message,
) -> actor.Next(WorkerState, Message) {
  case message {
    Poll -> {
      let result =
        task.claim(
          state.config.db,
          state.config.queue_name,
          state.config.worker_id,
          state.config.claim_timeout,
          state.config.batch_size,
        )

      case result {
        Ok(claims) -> {
          let state = WorkerState(..state, consecutive_errors: 0)

          list.each(claims, fn(claim) {
            execute_task(state, claim)
          })

          // Schedule next poll
          let _ =
            process.send_after(state.subject, state.config.poll_interval, Poll)
          actor.continue(state)
        }

        Error(error) -> {
          let errors = state.consecutive_errors + 1
          let state = WorkerState(..state, consecutive_errors: errors)
          // Exponential backoff: min(poll_interval * 2^errors, max_backoff)
          let backoff =
            int.min(
              exponential_backoff(state.config.poll_interval, errors),
              state.config.max_backoff,
            )
          log_error("claim error", error)
          let _ =
            process.send_after(state.subject, backoff, Poll)
          actor.continue(state)
        }
      }
    }

    Shutdown -> actor.stop()
  }
}

// ============================================================================
// Task Execution
// ============================================================================

fn execute_task(state: WorkerState, claim: Claim) -> Nil {
  case dict.get(state.handler_map, claim.task_name) {
    Ok(handler) -> {
      case handler.execute(claim) {
        Ok(result_json) -> {
          case
            task.complete(
              state.config.db,
              state.config.queue_name,
              claim.run_id,
              result_json,
            )
          {
            Ok(_) -> Nil
            Error(error) -> handle_completion_error("complete", error)
          }
        }
        Error(error_json) -> {
          // Call on_error hook if present
          case handler.on_error {
            option.Some(hook) -> hook(claim, error_json)
            option.None -> Nil
          }
          case
            task.fail(
              state.config.db,
              state.config.queue_name,
              claim.run_id,
              error_json,
            )
          {
            Ok(_) -> Nil
            Error(error) -> handle_completion_error("fail", error)
          }
        }
      }
    }
    Error(_) -> {
      // No handler for this task — defer it to support rolling deployments.
      // This matches the official Absurd SDK behaviour: unknown tasks are
      // rescheduled with a delay rather than permanently failed, so that a
      // worker with the correct handler can pick it up after a deploy.
      let _ =
        task.schedule_run(
          state.config.db,
          state.config.queue_name,
          claim.run_id,
          60,
        )
      io.println_error(
        "gabsurd worker: deferred unknown task \""
          <> claim.task_name
          <> "\" (no handler registered)",
      )
    }
  }
}

/// Handle errors from complete/fail calls.
///
/// The Absurd schema raises SQLSTATE AB001 (cancelled) and AB002 (already
/// failed) when you try to complete or fail a run that is already in a
/// terminal state. The official SDKs silently swallow these errors. We
/// log unexpected errors but silently ignore terminal-state conflicts.
fn handle_completion_error(context: String, error: GabsurdError) -> Nil {
  case error {
    // AB001 / AB002 — run is already in a terminal state (cancelled or
    // failed). This can happen if the claim expired and another worker
    // already handled it. Silently ignore, matching official SDK behaviour.
    QueryError(reason) if reason == "AB002" -> Nil
    QueryError(reason) -> {
      // AB001 or other SQLSTATE errors — log but don't crash
      case string_starts_with(reason, "AB0") {
        True -> Nil
        False -> log_error(context, error)
      }
    }
    _ -> log_error(context, error)
  }
}

fn string_starts_with(haystack: String, prefix: String) -> Bool {
  let hay_len = string.length(haystack)
  let pre_len = string.length(prefix)
  case hay_len < pre_len {
    True -> False
    False -> {
      let prefix_slice = string.slice(haystack, 0, pre_len)
      prefix_slice == prefix
    }
  }
}

/// Calculate exponential backoff: base * 2^errors.
fn exponential_backoff(base: Int, errors: Int) -> Int {
  case errors {
    0 -> base
    _ -> base * power_of_2(errors)
  }
}

fn power_of_2(n: Int) -> Int {
  case n {
    0 -> 1
    _ -> 2 * power_of_2(n - 1)
  }
}

fn log_error(context: String, error: GabsurdError) -> Nil {
  let msg = case error {
    QueryError(reason) -> context <> ": query error: " <> reason
    UnexpectedRowCount(reason) -> context <> ": " <> reason
    NotFound -> context <> ": not found"
    ConnectionError(reason) -> context <> ": connection error: " <> reason
  }
  io.println_error("gabsurd worker: " <> msg)
}
