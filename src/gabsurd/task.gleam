//// Task lifecycle operations for the Absurd durable workflow system.
//// Provides high-level functions for spawning, claiming, completing,
//// failing, and cancelling tasks.

import gleam/json
import gleam/list
import gleam/option
import gleam/result
import gleam/time/timestamp
import gabsurd/client.{type Db}
import gabsurd/sql

// ============================================================================
// Public Types
// ============================================================================

/// Information returned when spawning a task.
pub type SpawnInfo {
  SpawnInfo(task_id: BitArray, run_id: BitArray, attempt: Int, created: Bool)
}

/// Information returned when claiming a task.
pub type Claim {
  Claim(
    run_id: BitArray,
    task_id: BitArray,
    attempt: Int,
    task_name: String,
    params: String,
    retry_strategy: String,
    max_attempts: Int,
    headers: String,
    wake_event: String,
    event_payload: String,
  )
}

/// Options for spawning or retrying a task.
///
/// Create with `new_options()` and customize with `with_*` functions.
///
/// ## Example
///
/// ```gleam
/// let opts =
///   task.new_options()
///   |> task.with_max_attempts(3)
///   |> task.with_retry_strategy(task.FixedRetry(base_seconds: 30))
///   |> task.with_idempotency_key("order-123")
/// ```
pub type SpawnOptions {
  SpawnOptions(
    max_attempts: option.Option(Int),
    retry_strategy: option.Option(RetryStrategy),
    cancellation: option.Option(Cancellation),
    headers: option.Option(json.Json),
    idempotency_key: option.Option(String),
  )
}

/// Retry strategy for failed tasks.
pub type RetryStrategy {
  /// Retry with a fixed delay between attempts.
  FixedRetry(base_seconds: Int)
  /// Retry with exponential backoff.
  /// `base_seconds` is the initial delay, `factor` is the multiplier,
  /// `max_seconds` caps the delay.
  ExponentialRetry(
    base_seconds: Int,
    factor: Float,
    max_seconds: option.Option(Float),
  )
}

/// Cancellation policy for a task.
pub type Cancellation {
  /// Cancel the task if it runs longer than `max_duration` seconds.
  Cancellation(max_duration: Int)
}

// ============================================================================
// Options Builders
// ============================================================================

/// Create empty spawn options (all fields default to absent).
pub fn new_options() -> SpawnOptions {
  SpawnOptions(
    max_attempts: option.None,
    retry_strategy: option.None,
    cancellation: option.None,
    headers: option.None,
    idempotency_key: option.None,
  )
}

/// Set the maximum number of attempts for this task.
pub fn with_max_options(options: SpawnOptions, max: Int) -> SpawnOptions {
  SpawnOptions(..options, max_attempts: option.Some(max))
}

/// Set the retry strategy for failed tasks.
pub fn with_retry_strategy(
  options: SpawnOptions,
  strategy: RetryStrategy,
) -> SpawnOptions {
  SpawnOptions(..options, retry_strategy: option.Some(strategy))
}

/// Set the cancellation policy.
pub fn with_cancellation(
  options: SpawnOptions,
  cancellation: Cancellation,
) -> SpawnOptions {
  SpawnOptions(..options, cancellation: option.Some(cancellation))
}

/// Set headers (arbitrary JSON metadata).
pub fn with_headers(
  options: SpawnOptions,
  headers: json.Json,
) -> SpawnOptions {
  SpawnOptions(..options, headers: option.Some(headers))
}

/// Set an idempotency key to prevent duplicate task creation.
pub fn with_idempotency_key(
  options: SpawnOptions,
  key: String,
) -> SpawnOptions {
  SpawnOptions(..options, idempotency_key: option.Some(key))
}

/// Encode spawn options to a JSON string for the database.
pub fn encode_options(options: SpawnOptions) -> String {
  let entries = []
  let entries = case options.max_attempts {
    option.Some(max) -> [#("max_attempts", json.int(max)), ..entries]
    option.None -> entries
  }
  let entries = case options.retry_strategy {
    option.Some(strategy) ->
      [#("retry_strategy", encode_retry_strategy(strategy)), ..entries]
    option.None -> entries
  }
  let entries = case options.cancellation {
    option.Some(c) ->
      [#("cancellation", encode_cancellation(c)), ..entries]
    option.None -> entries
  }
  let entries = case options.headers {
    option.Some(h) -> [#("headers", h), ..entries]
    option.None -> entries
  }
  let entries = case options.idempotency_key {
    option.Some(key) ->
      [#("idempotency_key", json.string(key)), ..entries]
    option.None -> entries
  }
  json.to_string(json.object(entries))
}

fn encode_retry_strategy(strategy: RetryStrategy) -> json.Json {
  case strategy {
    FixedRetry(base_seconds) ->
      json.object([
        #("kind", json.string("fixed")),
        #("base_seconds", json.int(base_seconds)),
      ])
    ExponentialRetry(base_seconds, factor, max_seconds) -> {
      let entries = [
        #("kind", json.string("exponential")),
        #("base_seconds", json.int(base_seconds)),
        #("factor", json.float(factor)),
      ]
      let entries = case max_seconds {
        option.Some(max) ->
          [#("max_seconds", json.float(max)), ..entries]
        option.None -> entries
      }
      json.object(entries)
    }
  }
}

fn encode_cancellation(c: Cancellation) -> json.Json {
  case c {
    Cancellation(max_duration) ->
      json.object([#("max_duration", json.int(max_duration))])
  }
}

// ============================================================================
// Task Operations
// ============================================================================

/// Spawn a new task in a queue with typed options.
///
/// `params` is a `json.Json` value — use `json.object`, `json.string`, etc.
/// to build it. `options` is a `SpawnOptions` record — use `new_options()` and
/// `with_*` builders.
///
/// ## Example
///
/// ```gleam
/// let assert Ok(info) = task.spawn(
///   db,
///   "emails",
///   "send_welcome",
///   json.object([#("to", json.string("user@example.com"))]),
///   task.new_options() |> task.with_max_options(3),
/// )
/// ```
pub fn spawn(
  db: Db,
  queue_name: String,
  task_name: String,
  params: json.Json,
  options: SpawnOptions,
) -> Result(SpawnInfo, Nil) {
  let params_str = json.to_string(params)
  let options_str = encode_options(options)
  use row <- result.try(
    client.query_one(
      db,
      sql.spawn_task(queue_name, task_name, params_str, options_str),
    ),
  )
  Ok(SpawnInfo(
    task_id: row.task_id,
    run_id: row.run_id,
    attempt: row.attempt,
    created: row.created,
  ))
}

/// Claim available tasks from a queue for a worker.
pub fn claim(
  db: Db,
  queue_name: String,
  worker_id: String,
  claim_timeout: Int,
  qty: Int,
) -> Result(List(Claim), Nil) {
  use rows <- result.try(
    client.query_many(
      db,
      sql.claim_task(queue_name, worker_id, claim_timeout, qty),
    ),
  )
  Ok(
    list.map(rows, fn(row) {
      Claim(
        run_id: row.run_id,
        task_id: row.task_id,
        attempt: row.attempt,
        task_name: row.task_name,
        params: row.params,
        retry_strategy: row.retry_strategy,
        max_attempts: row.max_attempts,
        headers: row.headers,
        wake_event: row.wake_event,
        event_payload: row.event_payload,
      )
    }),
  )
}

/// Mark a run as completed with optional result state.
pub fn complete(
  db: Db,
  queue_name: String,
  run_id: BitArray,
  state: json.Json,
) -> Result(Nil, Nil) {
  client.exec(
    db,
    sql.complete_run(queue_name, run_id, json.to_string(state)),
  )
}

/// Mark a run as failed with a reason.
/// Passes NULL for retry_at so the queue's retry policy controls retries.
pub fn fail(
  db: Db,
  queue_name: String,
  run_id: BitArray,
  reason: json.Json,
) -> Result(Nil, Nil) {
  client.exec(
    db,
    sql.fail_run(queue_name, run_id, json.to_string(reason)),
  )
}

/// Mark a run as failed and schedule a retry at a specific time.
pub fn fail_with_retry(
  db: Db,
  queue_name: String,
  run_id: BitArray,
  reason: json.Json,
  retry_at: timestamp.Timestamp,
) -> Result(Nil, Nil) {
  client.exec(
    db,
    sql.fail_run_with_retry(
      queue_name,
      run_id,
      json.to_string(reason),
      retry_at,
    ),
  )
}

/// Cancel a task by its task_id.
pub fn cancel(
  db: Db,
  queue_name: String,
  task_id: BitArray,
) -> Result(Nil, Nil) {
  client.exec(db, sql.cancel_task(queue_name, task_id))
}

/// Get the result of a completed task.
pub fn get_result(
  db: Db,
  queue_name: String,
  task_id: BitArray,
) -> Result(#(String, String, String), Nil) {
  use row <- result.try(
    client.query_one(
      db,
      sql.get_task_result(queue_name, task_id),
    ),
  )
  Ok(#(row.state, row.result, row.failure_reason))
}

/// Retry a task with typed options.
pub fn retry(
  db: Db,
  queue_name: String,
  task_id: BitArray,
  options: SpawnOptions,
) -> Result(SpawnInfo, Nil) {
  let options_str = encode_options(options)
  use row <- result.try(
    client.query_one(
      db,
      sql.retry_task(queue_name, task_id, options_str),
    ),
  )
  Ok(SpawnInfo(
    task_id: row.task_id,
    run_id: row.run_id,
    attempt: row.attempt,
    created: row.created,
  ))
}
