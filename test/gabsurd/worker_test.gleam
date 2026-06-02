//// Tests for the OTP worker actor.

import gabsurd/client
import gabsurd/queue
import gabsurd/task
import gabsurd/worker
import gleam/erlang/process
import gleam/list
import gleam/option
import gleam/otp/static_supervisor
import gleeunit/should

const db_url = "postgresql://gabsurd:gabsurd@127.0.0.1:5432/gabsurd"

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Track how many times a handler is called, using process subject.
fn tracking_handler(
  task_name: String,
  tracker: process.Subject(Nil),
) -> worker.Handler {
  worker.Handler(
    task_name: task_name,
    execute: fn(_claim) {
      process.send(tracker, Nil)
      Ok("{\"tracked\": true}")
    },
    on_error: option.None,
  )
}

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Worker should claim a task, execute the handler, and complete it.
pub fn worker_claims_and_completes_task_test() {
  let #(db, q) = setup("test_worker_claim")

  // Spawn a task
  let assert Ok(_) = task.spawn(db, q, "ok_task", "{}", "{}")

  // Start a worker with a tracker so we know when it processes
  let tracker = process.new_subject()
  let handler = tracking_handler("ok_task", tracker)

  let config =
    worker.new(db, q, [handler])
    |> worker.with_poll_interval(100)

  let assert Ok(started) = worker.start(config)
  let w = started.data

  // Wait for the worker to process the task
  let assert Ok(Nil) = process.receive(tracker, 5000)

  // Verify: no more tasks to claim (it was completed)
  let assert Ok(claims) = task.claim(db, q, "verify_worker", 1, 10)
  claims |> list.length |> should.equal(0)

  worker.stop(w)
  teardown(db, q)
}

/// Worker should fail a task when handler returns Error.
pub fn worker_fails_task_on_handler_error_test() {
  let #(db, q) = setup("test_worker_fail")

  // Spawn with max_attempts=1 so fail_run doesn't create a retry
  let assert Ok(_) =
    task.spawn(db, q, "fail_task", "{}", "{\"max_attempts\": 1}")

  let tracker = process.new_subject()
  // A handler for fail_task that tracks when called, then returns error
  let handler =
    worker.Handler(
      task_name: "fail_task",
      execute: fn(_claim) {
        process.send(tracker, Nil)
        Error("{\"deliberate\": true}")
      },
      on_error: option.None,
    )

  let config =
    worker.new(db, q, [handler])
    |> worker.with_poll_interval(100)

  let assert Ok(started) = worker.start(config)
  let w = started.data
  let assert Ok(Nil) = process.receive(tracker, 5000)

  // Give it a moment to call task.fail
  process.sleep(200)

  // The task should now be permanently failed (max_attempts=1 exhausted)
  let assert Ok(claims) = task.claim(db, q, "verify_worker", 1, 10)
  claims |> list.length |> should.equal(0)

  worker.stop(w)
  teardown(db, q)
}

/// Worker should process multiple tasks sequentially.
pub fn worker_processes_multiple_tasks_test() {
  let #(db, q) = setup("test_worker_multi")

  // Spawn 3 tasks
  let assert Ok(_) = task.spawn(db, q, "ok_task", "{}", "{}")
  let assert Ok(_) = task.spawn(db, q, "ok_task", "{}", "{}")
  let assert Ok(_) = task.spawn(db, q, "ok_task", "{}", "{}")

  let tracker = process.new_subject()
  let handler = tracking_handler("ok_task", tracker)

  let config =
    worker.new(db, q, [handler])
    |> worker.with_poll_interval(100)
    |> worker.with_batch_size(3)

  let assert Ok(started) = worker.start(config)
  let w = started.data

  // Wait for all 3 tasks to be processed
  let assert Ok(Nil) = process.receive(tracker, 5000)
  let assert Ok(Nil) = process.receive(tracker, 5000)
  let assert Ok(Nil) = process.receive(tracker, 5000)

  // All should be completed
  let assert Ok(claims) = task.claim(db, q, "verify_worker", 1, 10)
  claims |> list.length |> should.equal(0)

  worker.stop(w)
  teardown(db, q)
}

/// Worker should fail tasks with no matching handler.
pub fn worker_fails_unknown_task_test() {
  let #(db, q) = setup("test_worker_unknown")

  // Spawn with max_attempts=1 so fail_run doesn't create a retry
  let assert Ok(_) =
    task.spawn(db, q, "unknown_task", "{}", "{\"max_attempts\": 1}")

  let tracker = process.new_subject()
  let handler = tracking_handler("ok_task", tracker)

  let config =
    worker.new(db, q, [handler])
    |> worker.with_poll_interval(100)

  let assert Ok(started) = worker.start(config)
  let w = started.data

  // Wait a bit for the poll cycle to run
  process.sleep(1000)

  // The unknown task should have been permanently failed by the worker
  let assert Ok(claims) = task.claim(db, q, "verify_worker", 1, 10)
  claims |> list.length |> should.equal(0)

  worker.stop(w)
  teardown(db, q)
}

/// Worker pool with multiple workers should process tasks concurrently.
pub fn worker_pool_processes_tasks_test() {
  let #(db, q) = setup("test_worker_pool")

  // Spawn 5 tasks
  let assert Ok(_) = task.spawn(db, q, "ok_task", "{}", "{}")
  let assert Ok(_) = task.spawn(db, q, "ok_task", "{}", "{}")
  let assert Ok(_) = task.spawn(db, q, "ok_task", "{}", "{}")
  let assert Ok(_) = task.spawn(db, q, "ok_task", "{}", "{}")
  let assert Ok(_) = task.spawn(db, q, "ok_task", "{}", "{}")

  let tracker = process.new_subject()
  let handler = tracking_handler("ok_task", tracker)

  let config =
    worker.new(db, q, [handler])
    |> worker.with_poll_interval(100)
    |> worker.with_batch_size(5)

  // Build a pool of 2 workers in a supervisor
  let pool = worker.pool_child_specs("pool_test", config, 2)

  let assert Ok(started) =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pool_first(pool))
    |> static_supervisor.add(pool_second(pool))
    |> static_supervisor.start()

  // Wait for all 5 tasks to be processed
  let assert Ok(Nil) = process.receive(tracker, 5000)
  let assert Ok(Nil) = process.receive(tracker, 5000)
  let assert Ok(Nil) = process.receive(tracker, 5000)
  let assert Ok(Nil) = process.receive(tracker, 5000)
  let assert Ok(Nil) = process.receive(tracker, 5000)

  // All should be completed
  let assert Ok(claims) = task.claim(db, q, "verify_worker", 1, 10)
  claims |> list.length |> should.equal(0)

  process.send_exit(started.pid)
  teardown(db, q)
}

fn pool_first(pool: List(a)) -> a {
  let assert Ok(x) = list.first(pool)
  x
}

fn pool_second(pool: List(a)) -> a {
  let assert [_, x, ..] = pool
  x
}
