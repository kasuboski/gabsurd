//// Tests for task lifecycle operations (spawn, claim, complete, fail, cancel).

import gabsurd/client
import gabsurd/queue
import gabsurd/task
import gleam/bit_array
import gleam/erlang/process
import gleam/json
import gleam/list
import gleeunit/should

const db_url = "postgresql://gabsurd:gabsurd@127.0.0.1:5432/gabsurd"

/// Helper: start a DB connection + create a queue, return both.
fn setup(queue_name: String) -> #(client.Db, String) {
  let assert Ok(started) = client.start(db_url)
  let db = started.data
  let assert Ok(Nil) = queue.create(db, queue_name)
  #(db, queue_name)
}

/// Helper: cleanup queue + stop connection.
fn teardown(db: client.Db, queue_name: String) {
  let _ = queue.drop(db, queue_name)
  process.send_exit(db.pid)
}

pub fn spawn_task_test() {
  let #(db, q) = setup("test_spawn")

  let assert Ok(task_info) =
    task.spawn(db, q, "my_task", json.object([]), task.new_options())
  task_info.attempt |> should.equal(1)
  task_info.created |> should.be_true
  should.be_true(bit_array.byte_size(task_info.task_id) == 16)
  should.be_true(bit_array.byte_size(task_info.run_id) == 16)

  teardown(db, q)
}

pub fn spawn_with_options_test() {
  let #(db, q) = setup("test_spawn_opts")

  let assert Ok(task_info) =
    task.spawn(
      db,
      q,
      "retry_task",
      json.object([#("url", json.string("http://example.com"))]),
      task.new_options()
      |> task.with_max_attempts(3),
    )
  task_info.attempt |> should.equal(1)
  task_info.created |> should.be_true

  teardown(db, q)
}

pub fn claim_task_test() {
  let #(db, q) = setup("test_claim")

  let assert Ok(task_info) =
    task.spawn(db, q, "claimable_task", json.object([]), task.new_options())
  let assert Ok(claims) = task.claim(db, q, "worker-1", 30, 1)
  list.length(claims) |> should.equal(1)

  let assert Ok(claim) = list.first(claims)
  claim.task_name |> should.equal("claimable_task")
  claim.attempt |> should.equal(1)
  claim.run_id |> should.equal(task_info.run_id)

  teardown(db, q)
}

pub fn complete_run_test() {
  let #(db, q) = setup("test_complete")

  let assert Ok(_) =
    task.spawn(db, q, "completable_task", json.object([]), task.new_options())
  let assert Ok(claims) = task.claim(db, q, "worker-2", 30, 1)
  let assert Ok(claim) = list.first(claims)

  task.complete(
    db,
    q,
    claim.run_id,
    json.object([#("status", json.string("done"))]),
  )
  |> should.be_ok

  teardown(db, q)
}

pub fn fail_run_test() {
  let #(db, q) = setup("test_fail")

  let assert Ok(_) =
    task.spawn(db, q, "failing_task", json.object([]), task.new_options())
  let assert Ok(claims) = task.claim(db, q, "worker-3", 30, 1)
  let assert Ok(claim) = list.first(claims)

  task.fail(
    db,
    q,
    claim.run_id,
    json.object([#("error", json.string("boom"))]),
  )
  |> should.be_ok

  teardown(db, q)
}

pub fn cancel_task_test() {
  let #(db, q) = setup("test_cancel")

  let assert Ok(task_info) =
    task.spawn(db, q, "cancellable_task", json.object([]), task.new_options())
  task.cancel(db, q, task_info.task_id) |> should.be_ok

  teardown(db, q)
}

/// Test the full lifecycle: spawn → claim → complete
pub fn full_lifecycle_test() {
  let #(db, q) = setup("test_lifecycle")

  // 1. Spawn
  let assert Ok(spawned) =
    task.spawn(
      db,
      q,
      "lifecycle_task",
      json.object([#("input", json.int(42))]),
      task.new_options(),
    )
  spawned.attempt |> should.equal(1)

  // 2. Claim
  let assert Ok(claims) = task.claim(db, q, "lifecycle-worker", 30, 1)
  list.length(claims) |> should.equal(1)
  let assert Ok(claim) = list.first(claims)
  claim.task_name |> should.equal("lifecycle_task")

  // 3. Complete
  let assert Ok(Nil) =
    task.complete(
      db,
      q,
      claim.run_id,
      json.object([#("output", json.int(99))]),
    )

  // 4. Verify no more tasks to claim
  let assert Ok(claims2) = task.claim(db, q, "lifecycle-worker", 30, 1)
  list.length(claims2) |> should.equal(0)

  teardown(db, q)
}
