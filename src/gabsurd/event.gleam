//// Event operations for the Absurd durable workflow system.
//// Provides functions for emitting and awaiting events.

import gabsurd/client.{type Db}
import gabsurd/sql

/// Result of an await_event call.
pub type AwaitResult {
  AwaitResult(should_suspend: Bool, payload: String)
}

/// Emit an event to a queue.
pub fn emit(
  db: Db,
  queue_name: String,
  event_name: String,
  payload: String,
) -> Result(Nil, Nil) {
  client.exec(db, sql.emit_event(queue_name, event_name, payload))
}

/// Await an event for a specific task step.
/// Returns an AwaitResult indicating whether the task should suspend
/// (no event available) or continue (event received).
pub fn await(
  db: Db,
  queue_name: String,
  task_id: BitArray,
  run_id: BitArray,
  step_name: String,
  event_name: String,
  timeout: Int,
) -> Result(AwaitResult, Nil) {
  use row <- result.try(client.query_one(
    db,
    sql.await_event(queue_name, task_id, run_id, step_name, event_name, timeout),
  ))
  Ok(AwaitResult(should_suspend: row.should_suspend, payload: row.payload))
}

import gleam/result
