//// Tests for checkpoint operations.

import gabsurd/checkpoint
import gabsurd/client
import gabsurd/queue
import gabsurd/task
import gleam/erlang/process
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

  let assert Ok(spawned) = task.spawn(db, q, "checkpoint_task", "{}", "{}")
  let assert Ok(claims) = task.claim(db, q, "cp-worker", 30, 1)
  let assert Ok(claim) = list.first(claims)

  // Set a checkpoint
  checkpoint.set(
    db,
    q,
    spawned.task_id,
    "step1",
    "{\"progress\": 50}",
    claim.run_id,
  )
  |> should.be_ok

  teardown(db, q)
}

pub fn get_checkpoint_test() {
  let #(db, q) = setup("test_checkpoint_get")

  let assert Ok(spawned) = task.spawn(db, q, "cp_get_task", "{}", "{}")
  let assert Ok(claims) = task.claim(db, q, "cp-get-worker", 30, 1)
  let assert Ok(claim) = list.first(claims)

  // Set a checkpoint
  let assert Ok(Nil) =
    checkpoint.set(
      db,
      q,
      spawned.task_id,
      "step1",
      "{\"progress\": 75}",
      claim.run_id,
    )

  // Get it back
  let assert Ok(cp) = checkpoint.get(db, q, spawned.task_id, "step1", False)
  cp.checkpoint_name |> should.equal("step1")
  cp.state |> should.equal("{\"progress\": 75}")
  cp.status |> should.equal("committed")

  teardown(db, q)
}
