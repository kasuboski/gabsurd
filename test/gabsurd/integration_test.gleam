//// Full integration test for queue management.

import gabsurd/client
import gabsurd/queue
import gleam/erlang/process
import gleam/io
import gleam/list
import gleeunit/should

const db_url = "postgresql://gabsurd:gabsurd@127.0.0.1:5432/gabsurd"

pub fn integration_test() {
  let assert Ok(started) = client.start(db_url)
  let db = started.data

  // Create a queue
  let result = queue.create(db, "it_queue")
  result |> should.be_ok
  io.println("✅ create_queue")

  // List queues - should contain our queue
  let assert Ok(queues) = queue.list(db)
  queues |> list.contains("it_queue") |> should.be_true
  io.println("✅ list_queues")

  // Drop the queue
  let result = queue.drop(db, "it_queue")
  result |> should.be_ok
  io.println("✅ drop_queue")

  // List again - should not contain our queue
  let assert Ok(queues) = queue.list(db)
  queues |> list.contains("it_queue") |> should.be_false
  io.println("✅ queue removed from list")

  process.send_exit(db.pid)
}
