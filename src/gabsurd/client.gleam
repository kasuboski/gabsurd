//// Database client for gabsurd — wraps pog connection and provides
//// helpers for executing parrot-generated queries against PostgreSQL.

import gabsurd/param
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/string
import parrot/dev
import pog

/// A database connection handle.
pub type Db {
  Db(pid: process.Pid, connection: pog.Connection)
}

/// Errors returned by gabsurd operations.
pub type GabsurdError {
  /// The database query failed (connection issue, constraint violation, etc.)
  QueryError(String)
  /// A query returned an unexpected number of rows.
  UnexpectedRowCount(String)
  /// No row was found for a `:one` query.
  NotFound
  /// Failed to connect to the database.
  ConnectionError(String)
}

/// Generate a unique integer for pool names.
@external(erlang, "erlang", "unique_integer")
pub fn unique_integer() -> Int

/// Start a database connection from a DATABASE_URL string.
/// Uses a unique pool name to avoid collisions when multiple
/// connections are started (e.g., in tests).
pub fn start(url: String) -> actor.StartResult(Db) {
  let id = unique_integer()
  let name = process.new_name("gabsurd_" <> int.to_string(id))
  let config_result = pog.url_config(name, url)
  case config_result {
    Error(_) -> Error(actor.InitFailed("invalid DATABASE_URL"))
    Ok(config) -> {
      let config = config |> pog.pool_size(2)
      case pog.start(config) {
        Ok(started) ->
          Ok(actor.Started(
            pid: started.pid,
            data: Db(pid: started.pid, connection: started.data),
          ))
        Error(reason) -> Error(reason)
      }
    }
  }
}

/// Start a connection from individual parts (useful for testing).
pub fn start_with(
  host: String,
  port: Int,
  database: String,
  user: String,
  password: String,
) -> actor.StartResult(Db) {
  let id = unique_integer()
  let name = process.new_name("gabsurd_" <> int.to_string(id))
  let config =
    pog.default_config(pool_name: name)
    |> pog.host(host)
    |> pog.port(port)
    |> pog.database(database)
    |> pog.user(user)
    |> pog.password(option.Some(password))
    |> pog.ssl(pog.SslDisabled)
    |> pog.pool_size(2)

  case pog.start(config) {
    Ok(started) ->
      Ok(actor.Started(
        pid: started.pid,
        data: Db(pid: started.pid, connection: started.data),
      ))
    Error(reason) -> Error(reason)
  }
}

/// Get the underlying pog connection.
pub fn connection(db: Db) -> pog.Connection {
  db.connection
}

/// Execute a `:one` query — returns a single row of type `a`, or Error if no row.
pub fn query_one(
  db: Db,
  sql_params_decoder: #(String, List(dev.Param), decode.Decoder(a)),
) -> Result(a, GabsurdError) {
  let #(sql, params, decoder) = sql_params_decoder
  let result =
    sql
    |> pog.query()
    |> pog.returning(decoder)
    |> add_params(params)
    |> pog.execute(db.connection)

  case result {
    Ok(pog.Returned(1, [row])) -> Ok(row)
    // Absurd uses FOR UPDATE SKIP LOCKED so claim_task will never return
    // duplicate rows for the same run, but we handle the unexpected case.
    Ok(pog.Returned(n, _)) ->
      Error(UnexpectedRowCount("expected 1 row, got " <> int.to_string(n)))
    Error(error) ->
      Error(query_error_to_gabsurd(error))
  }
}

/// Execute a `:many` query — returns a list of rows of type `a`.
pub fn query_many(
  db: Db,
  sql_params_decoder: #(String, List(dev.Param), decode.Decoder(a)),
) -> Result(List(a), GabsurdError) {
  let #(sql, params, decoder) = sql_params_decoder
  let result =
    sql
    |> pog.query()
    |> pog.returning(decoder)
    |> add_params(params)
    |> pog.execute(db.connection)

  case result {
    Ok(pog.Returned(_, rows)) -> Ok(rows)
    Error(error) ->
      Error(query_error_to_gabsurd(error))
  }
}

/// Execute a `:exec` query — returns Nil on success.
/// Void-returning functions are cast to ::text in SQL so pgo can decode them.
pub fn exec(
  db: Db,
  sql_and_params: #(String, List(dev.Param)),
) -> Result(Nil, GabsurdError) {
  let #(sql, params) = sql_and_params
  let result =
    sql
    |> pog.query()
    |> pog.returning(decode.at([0], decode.optional(decode.string)))
    |> add_params(params)
    |> pog.execute(db.connection)

  case result {
    Ok(_) -> Ok(Nil)
    Error(error) ->
      Error(query_error_to_gabsurd(error))
  }
}

fn add_params(query: pog.Query(a), params: List(dev.Param)) -> pog.Query(a) {
  list.fold(params, query, fn(acc, p) { pog.parameter(acc, param.to_pog(p)) })
}

fn query_error_to_gabsurd(error: pog.QueryError) -> GabsurdError {
  case error {
    pog.ConstraintViolated(message, _, _) -> QueryError(message)
    pog.PostgresqlError(code, _, message) ->
      QueryError(code <> ": " <> message)
    pog.UnexpectedArgumentCount(expected, got) ->
      QueryError(
        "unexpected argument count: expected "
          <> int.to_string(expected)
          <> " got "
          <> int.to_string(got),
      )
    pog.UnexpectedArgumentType(expected, got) ->
      QueryError("unexpected argument type: expected " <> expected <> " got " <> got)
    pog.UnexpectedResultType(errors) ->
      QueryError(
        "decode error: "
          <> string.join(
            list.map(errors, fn(e) {
              e.expected <> " got " <> e.found
            }),
            ", "),
      )
    pog.QueryTimeout -> QueryError("query timeout")
    pog.ConnectionUnavailable -> ConnectionError("no connection available")
  }
}
