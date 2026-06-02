//// Tests for utility operations.

import gabsurd/client
import gabsurd/utility
import gleam/erlang/process
import gleam/string
import gleeunit/should

const db_url = "postgresql://gabsurd:gabsurd@127.0.0.1:5432/gabsurd"

pub fn get_schema_version_test() {
  let assert Ok(started) = client.start(db_url)
  let db = started.data

  let assert Ok(version) = utility.get_schema_version(db)
  should.be_true(string.length(version) > 0)

  process.send_exit(db.pid)
}
