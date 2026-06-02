//// Task lifecycle operations for the Absurd durable workflow system.
//// Provides high-level functions for spawning, claiming, completing,
//// failing, and cancelling tasks.

import gabsurd/client.{type Db}
import gabsurd/sql
import gleam/list
import gleam/result
import gleam/time/timestamp

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

/// Spawn a new task in a queue.
pub fn spawn(
  db: Db,
  queue_name: String,
  task_name: String,
  params: String,
  options: String,
) -> Result(SpawnInfo, Nil) {
  use row <- result.try(client.query_one(
    db,
    sql.spawn_task(queue_name, task_name, params, options),
  ))
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
  use rows <- result.try(client.query_many(
    db,
    sql.claim_task(queue_name, worker_id, claim_timeout, qty),
  ))
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
  state: String,
) -> Result(Nil, Nil) {
  client.exec(db, sql.complete_run(queue_name, run_id, state))
}

/// Mark a run as failed with a reason.
/// Passes NULL for retry_at so the queue's retry policy controls retries.
pub fn fail(
  db: Db,
  queue_name: String,
  run_id: BitArray,
  reason: String,
) -> Result(Nil, Nil) {
  client.exec(db, sql.fail_run(queue_name, run_id, reason))
}

/// Mark a run as failed and schedule a retry at a specific time.
pub fn fail_with_retry(
  db: Db,
  queue_name: String,
  run_id: BitArray,
  reason: String,
  retry_at: timestamp.Timestamp,
) -> Result(Nil, Nil) {
  client.exec(db, sql.fail_run_with_retry(queue_name, run_id, reason, retry_at))
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
  use row <- result.try(client.query_one(
    db,
    sql.get_task_result(queue_name, task_id),
  ))
  Ok(#(row.state, row.result, row.failure_reason))
}

/// Retry a task.
pub fn retry(
  db: Db,
  queue_name: String,
  task_id: BitArray,
  options: String,
) -> Result(SpawnInfo, Nil) {
  use row <- result.try(client.query_one(
    db,
    sql.retry_task(queue_name, task_id, options),
  ))
  Ok(SpawnInfo(
    task_id: row.task_id,
    run_id: row.run_id,
    attempt: row.attempt,
    created: row.created,
  ))
}
