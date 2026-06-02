//// OTP Worker actor for the Absurd durable workflow system.
//// Polls a queue for tasks, dispatches to registered handlers,
//// and completes/fails tasks based on handler results.

import gabsurd/client.{type Db}
import gabsurd/task.{type Claim}
import gleam/dict
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/supervision

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
///     Ok("{\"sent\": true}")
///   },
///   on_error: None,
/// )
/// ```
pub type Handler {
  Handler(
    /// The task_name this handler responds to.
    task_name: String,
    /// Execute the task. Return `Ok(json)` to complete or `Error(json)` to fail.
    execute: fn(Claim) -> Result(String, String),
    /// Optional hook called when execute returns Error.
    /// Receives the claim and the error string. Use for logging/metrics.
    on_error: option.Option(fn(Claim, String) -> Nil),
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
      WorkerState(config: config, handler_map: handler_map, subject: subject)
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
/// Each worker gets a unique worker_id: `{name}_{i}`.
pub fn pool_child_specs(
  name: String,
  config: Config,
  count: Int,
) -> List(supervision.ChildSpecification(Worker)) {
  list_repeater(count, fn(i) {
    let config = Config(..config, worker_id: name <> "_" <> int.to_string(i))
    supervision.worker(fn() { start(config) })
  })
}

fn list_repeater(count: Int, f: fn(Int) -> a) -> List(a) {
  list_repeater_loop(count, f, [])
}

fn list_repeater_loop(i: Int, f: fn(Int) -> a, acc: List(a)) -> List(a) {
  case i <= 0 {
    True -> acc
    False -> list_repeater_loop(i - 1, f, [f(i), ..acc])
  }
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
          // Process each claimed task
          list.each(claims, fn(claim) { execute_task(state, claim) })
        }
        Error(_) -> Nil
      }

      // Schedule next poll
      let _ =
        process.send_after(state.subject, state.config.poll_interval, Poll)
      actor.continue(state)
    }

    Shutdown -> actor.stop()
  }
}

fn execute_task(state: WorkerState, claim: Claim) -> Nil {
  case dict.get(state.handler_map, claim.task_name) {
    Ok(handler) -> {
      case handler.execute(claim) {
        Ok(result_json) -> {
          let _ =
            task.complete(
              state.config.db,
              state.config.queue_name,
              claim.run_id,
              result_json,
            )
          Nil
        }
        Error(error_json) -> {
          // Call on_error hook if present
          case handler.on_error {
            option.Some(hook) -> hook(claim, error_json)
            option.None -> Nil
          }
          let _ =
            task.fail(
              state.config.db,
              state.config.queue_name,
              claim.run_id,
              error_json,
            )
          Nil
        }
      }
    }
    Error(_) -> {
      // No handler for this task — fail it
      let reason =
        "{\"error\": \"no_handler\", \"task_name\": \""
        <> claim.task_name
        <> "\"}"
      let _ =
        task.fail(
          state.config.db,
          state.config.queue_name,
          claim.run_id,
          reason,
        )
      Nil
    }
  }
}
