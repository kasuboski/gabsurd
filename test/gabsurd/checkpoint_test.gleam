//// Tests for checkpoint operations.

import gabsurd/checkpoint
import gabsurd/client
import gleam/option
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

pub fn set_checkpoint_test() {
  let #(db, q) = setup("test_checkpoint_set")

  let assert Ok(spawned) =
    task.spawn(db, q, "checkpoint_task", json.object([]), task.new_options())
  let assert Ok(claims) = task.claim(db, q, "cp-worker", 30, 1)
  let assert Ok(claim) = list.first(claims)

  // Set a checkpoint (extend_claim_by=0 for this test)
  checkpoint.set(
    db,
    q,
    spawned.task_id,
    "step1",
    json.object([#("progress", json.int(50))]),
    claim.run_id,
    0,
  )
  |> should.be_ok

  teardown(db, q)
}

pub fn get_checkpoint_test() {
  let #(db, q) = setup("test_checkpoint_get")

  let assert Ok(spawned) =
    task.spawn(db, q, "cp_get_task", json.object([]), task.new_options())
  let assert Ok(claims) = task.claim(db, q, "cp-get-worker", 30, 1)
  let assert Ok(claim) = list.first(claims)

  // Set a checkpoint (extend_claim_by=0 for this test)
  let assert Ok(Nil) =
    checkpoint.set(
      db,
      q,
      spawned.task_id,
      "step1",
      json.object([#("progress", json.int(75))]),
      claim.run_id,
      0,
    )

  // Get it back
  let assert Ok(option.Some(cp)) = checkpoint.get(db, q, spawned.task_id, "step1", False)
  cp.checkpoint_name |> should.equal("step1")
  cp.state |> should.equal("{\"progress\": 75}")
  cp.status |> should.equal("committed")

  teardown(db, q)
}
