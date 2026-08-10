//// Tests for task lifecycle operations (spawn, claim, complete, fail, cancel).

import gabsurd/client
import gabsurd/event
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

  task.fail(db, q, claim.run_id, json.object([#("error", json.string("boom"))]))
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

/// Task reads expose routing metadata and every practical task state.
pub fn task_result_states_test() {
  let #(db, q) = setup("test_result_states")
  let headers = json.object([#("source", json.string("test"))])

  // pending
  let assert Ok(pending) =
    task.spawn(
      db,
      q,
      "pending_task",
      json.object([]),
      task.new_options() |> task.with_headers(headers),
    )
  let assert Ok(pending_result) = task.get_result(db, q, pending.task_id)
  pending_result.task_name |> should.equal("pending_task")
  pending_result.headers |> should.equal("{\"source\": \"test\"}")
  pending_result.state |> should.equal("pending")
  let assert Ok(Nil) = task.cancel(db, q, pending.task_id)

  // running
  let assert Ok(running) =
    task.spawn(db, q, "running_task", json.object([]), task.new_options())
  let assert Ok(running_claims) = task.claim(db, q, "state-worker", 30, 1)
  let assert Ok(running_claim) = list.first(running_claims)
  running_claim.task_id |> should.equal(running.task_id)
  let assert Ok(running_result) = task.get_result(db, q, running.task_id)
  running_result.task_name |> should.equal("running_task")
  running_result.headers |> should.equal("{}")
  running_result.state |> should.equal("running")
  let assert Ok(Nil) =
    task.complete(db, q, running_claim.run_id, json.string("running-done"))

  // sleeping
  let assert Ok(sleeping) =
    task.spawn(db, q, "sleeping_task", json.object([]), task.new_options())
  let assert Ok(sleeping_claims) = task.claim(db, q, "state-worker", 30, 1)
  let assert Ok(sleeping_claim) = list.first(sleeping_claims)
  let assert Ok(awaited) =
    event.await(
      db,
      q,
      sleeping.task_id,
      sleeping_claim.run_id,
      "state-step",
      "state-event",
      1,
    )
  awaited.should_suspend |> should.be_true
  let assert Ok(sleeping_result) = task.get_result(db, q, sleeping.task_id)
  sleeping_result.task_name |> should.equal("sleeping_task")
  sleeping_result.headers |> should.equal("{}")
  sleeping_result.state |> should.equal("sleeping")

  // completed
  let assert Ok(completed) =
    task.spawn(db, q, "completed_task", json.object([]), task.new_options())
  let assert Ok(completed_claims) = task.claim(db, q, "state-worker", 30, 1)
  let assert Ok(completed_claim) = list.first(completed_claims)
  let assert Ok(Nil) =
    task.complete(db, q, completed_claim.run_id, json.string("done"))
  let assert Ok(completed_result) = task.get_result(db, q, completed.task_id)
  completed_result.task_name |> should.equal("completed_task")
  completed_result.headers |> should.equal("{}")
  completed_result.state |> should.equal("completed")
  completed_result.result |> should.equal("\"done\"")

  // failed
  let assert Ok(failed) =
    task.spawn(
      db,
      q,
      "failed_task",
      json.object([]),
      task.new_options() |> task.with_max_attempts(1),
    )
  let assert Ok(failed_claims) = task.claim(db, q, "state-worker", 30, 1)
  let assert Ok(failed_claim) = list.first(failed_claims)
  let assert Ok(Nil) =
    task.fail(db, q, failed_claim.run_id, json.string("boom"))
  let assert Ok(failed_result) = task.get_result(db, q, failed.task_id)
  failed_result.task_name |> should.equal("failed_task")
  failed_result.headers |> should.equal("{}")
  failed_result.state |> should.equal("failed")
  failed_result.failure_reason |> should.equal("\"boom\"")

  // cancelled
  let assert Ok(cancelled) =
    task.spawn(db, q, "cancelled_task", json.object([]), task.new_options())
  let assert Ok(Nil) = task.cancel(db, q, cancelled.task_id)
  let assert Ok(cancelled_result) = task.get_result(db, q, cancelled.task_id)
  cancelled_result.task_name |> should.equal("cancelled_task")
  cancelled_result.headers |> should.equal("{}")
  cancelled_result.state |> should.equal("cancelled")

  // A task ID from another queue is not found.
  let missing_queue = "test_result_missing"
  let assert Ok(Nil) = queue.create(db, missing_queue)
  task.get_result(db, missing_queue, pending.task_id)
  |> should.equal(Error(client.NotFound))
  let _ = queue.drop(db, missing_queue)
  teardown(db, q)
}

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
    task.complete(db, q, claim.run_id, json.object([#("output", json.int(99))]))

  // 4. Verify no more tasks to claim
  let assert Ok(claims2) = task.claim(db, q, "lifecycle-worker", 30, 1)
  list.length(claims2) |> should.equal(0)

  teardown(db, q)
}
