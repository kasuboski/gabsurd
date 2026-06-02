//// Checkpoint operations for the Absurd durable workflow system.
//// Provides functions for setting and getting task checkpoint state.

import gabsurd/client.{type Db}
import gabsurd/sql

/// A checkpoint record.
pub type Checkpoint {
  Checkpoint(
    checkpoint_name: String,
    state: String,
    status: String,
    owner_run_id: BitArray,
    updated_at: Timestamp,
  )
}

/// Set a checkpoint for a task step.
pub fn set(
  db: Db,
  queue_name: String,
  task_id: BitArray,
  step_name: String,
  state: String,
  owner_run_id: BitArray,
) -> Result(Nil, Nil) {
  client.exec(
    db,
    sql.set_task_checkpoint_state(
      queue_name,
      task_id,
      step_name,
      state,
      owner_run_id,
      0,
    ),
  )
}

/// Get a checkpoint for a task step.
pub fn get(
  db: Db,
  queue_name: String,
  task_id: BitArray,
  step_name: String,
  include_pending: Bool,
) -> Result(Checkpoint, Nil) {
  use row <- result.try(client.query_one(
    db,
    sql.get_task_checkpoint_state(
      queue_name,
      task_id,
      step_name,
      include_pending,
    ),
  ))
  Ok(Checkpoint(
    checkpoint_name: row.checkpoint_name,
    state: row.state,
    status: row.status,
    owner_run_id: row.owner_run_id,
    updated_at: row.updated_at,
  ))
}

import gleam/result
import gleam/time/timestamp.{type Timestamp}
