//// Tests for event operations (emit, await).

import gabsurd/client
import gabsurd/event
import gabsurd/queue
import gabsurd/task
import gleam/erlang/process
import gleam/json
import gleam/list
import gleeunit/should

const db_url = "postgresql://gabsurd:gabsurd@127.0.0.1:5432/gabsurd"

fn setup(queue_name: String) -> #(client.Db, String) {
  let assert Ok(started) = client.start(db_url)
  let db = started.data
  let assert Ok(Nil) = queue.create(db, queue_name)
  #(db, queue_name)
}

fn teardown(db: client.Db, queue_name: String) {
  let _ = queue.drop(db, queue_name)
  process.send_exit(db.pid)
}

pub fn emit_event_test() {
  let #(db, q) = setup("test_emit_event")

  // Emit should succeed even with no listeners
  event.emit(db, q, "test_event", json.object([#("key", json.string("value"))]))
  |> should.be_ok

  teardown(db, q)
}

pub fn emit_event_null_payload_test() {
  let #(db, q) = setup("test_emit_null")

  // Emit with JSON null payload
  event.emit(db, q, "null_event", json.null())
  |> should.be_ok

  teardown(db, q)
}

/// Test that await_event returns should_suspend=true when no event is pending
pub fn await_event_no_event_test() {
  let #(db, q) = setup("test_await_noevent")

  // Spawn + claim to get task/run IDs
  let assert Ok(spawned) =
    task.spawn(db, q, "awaiting_task", json.object([]), task.new_options())
  let assert Ok(claims) = task.claim(db, q, "await-worker", 30, 1)
  let assert Ok(claim) = list.first(claims)

  // Await with short timeout should return should_suspend=true
  let assert Ok(result) =
    event.await(db, q, spawned.task_id, claim.run_id, "step1", "my_event", 1)
  result.should_suspend |> should.be_true

  teardown(db, q)
}

/// Test emit-then-await: emit event first, then await should find it
pub fn emit_then_await_test() {
  let #(db, q) = setup("test_emit_await")

  let assert Ok(spawned) =
    task.spawn(db, q, "event_task", json.object([]), task.new_options())
  let assert Ok(claims) = task.claim(db, q, "event-worker", 30, 1)
  let assert Ok(claim) = list.first(claims)

  // Emit the event first
  let assert Ok(Nil) =
    event.emit(db, q, "expected_event", json.object([#("data", json.int(42))]))

  // Now await should find it immediately
  let assert Ok(result) =
    event.await(
      db,
      q,
      spawned.task_id,
      claim.run_id,
      "step1",
      "expected_event",
      5,
    )
  result.should_suspend |> should.be_false
  result.payload |> should.equal("{\"data\": 42}")

  teardown(db, q)
}
