//// Queue management for the Absurd durable workflow system.
//// Provides high-level functions for creating, dropping, and listing queues.

import gleam/list
import gleam/result
import gabsurd/client.{type Db, type GabsurdError}
import gabsurd/sql

/// Create a new queue with default (unpartitioned) storage mode.
pub fn create(db: Db, queue_name: String) -> Result(Nil, GabsurdError) {
  client.exec(db, sql.create_queue(queue_name))
}

/// Create a new queue with a specific storage mode.
pub fn create_with_mode(
  db: Db,
  queue_name: String,
  storage_mode: String,
) -> Result(Nil, GabsurdError) {
  client.exec(db, sql.create_queue_with_mode(queue_name, storage_mode))
}

/// Drop a queue and all its associated tables.
pub fn drop(db: Db, queue_name: String) -> Result(Nil, GabsurdError) {
  client.exec(db, sql.drop_queue(queue_name))
}

/// List all queue names.
pub fn list(db: Db) -> Result(List(String), GabsurdError) {
  use rows <- result.try(client.query_many(db, sql.list_queues()))
  Ok(rows |> list.map(fn(row) { row.queue_name }))
}
