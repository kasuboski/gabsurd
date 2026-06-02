-- ============================================================================
-- gabsurd: Parrot/sqlc query definitions for the Absurd durable workflow schema.
-- All queries target PL/pgSQL functions (not dynamic queue tables).
--
-- NOTE: sqlc struggles to infer return types from PL/pgSQL functions.
-- We must explicitly select and cast each column from RETURNS TABLE(...).
-- Nullable columns use COALESCE with aliases so parrot generates clean types.
-- Void-returning functions are cast to ::text to avoid pgo decode crashes
-- on unknown_oid (void returns empty binary which pgo can't decode).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Queue Management
-- ----------------------------------------------------------------------------

-- name: CreateQueue :exec
SELECT absurd.create_queue($1)::text;

-- name: CreateQueueWithMode :exec
SELECT absurd.create_queue($1, $2)::text;

-- name: DropQueue :exec
SELECT absurd.drop_queue($1)::text;

-- name: ListQueues :many
SELECT queue_name::text
FROM absurd.list_queues();

-- name: GetQueuePolicy :one
SELECT
  queue_name::text,
  storage_mode::text,
  default_partition::text,
  partition_lookahead::text,
  partition_lookback::text,
  cleanup_ttl::text,
  cleanup_limit::integer,
  detach_mode::text,
  detach_min_age::text
FROM absurd.get_queue_policy($1);

-- name: SetQueuePolicy :exec
SELECT absurd.set_queue_policy($1, $2)::text;

-- ----------------------------------------------------------------------------
-- Task Lifecycle
-- ----------------------------------------------------------------------------

-- name: SpawnTask :one
SELECT
  task_id::uuid,
  run_id::uuid,
  attempt::integer,
  created::boolean
FROM absurd.spawn_task($1, $2, $3, $4);

-- name: ClaimTask :many
SELECT
  run_id::uuid,
  task_id::uuid,
  attempt::integer,
  task_name::text,
  params::text,
  COALESCE(retry_strategy, '{}')::text AS retry_strategy,
  COALESCE(max_attempts, 0)::integer AS max_attempts,
  COALESCE(headers, '{}')::text AS headers,
  COALESCE(wake_event, '')::text AS wake_event,
  COALESCE(event_payload, '{}')::text AS event_payload
FROM absurd.claim_task($1, $2, $3, $4);

-- name: CompleteRun :exec
SELECT absurd.complete_run($1, $2, $3)::text;

-- name: ScheduleRun :exec
SELECT absurd.schedule_run($1, $2, $3)::text;

-- name: FailRun :exec
SELECT absurd.fail_run($1, $2, $3, NULL)::text;

-- name: FailRunWithRetry :exec
SELECT absurd.fail_run($1, $2, $3, $4)::text;

-- name: RetryTask :one
SELECT
  task_id::uuid,
  run_id::uuid,
  attempt::integer,
  created::boolean
FROM absurd.retry_task($1, $2, $3);

-- name: CancelTask :exec
SELECT absurd.cancel_task($1, $2)::text;

-- name: GetTaskResult :one
SELECT
  task_id::uuid,
  state::text,
  COALESCE(result, '{}')::text AS result,
  COALESCE(failure_reason, '{}')::text AS failure_reason
FROM absurd.get_task_result($1, $2);

-- ----------------------------------------------------------------------------
-- Checkpoints
-- ----------------------------------------------------------------------------

-- name: SetTaskCheckpointState :exec
SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)::text;

-- name: GetTaskCheckpointState :one
SELECT
  checkpoint_name::text,
  COALESCE(state, '{}')::text AS state,
  status::text,
  COALESCE(owner_run_id, '00000000-0000-0000-0000-000000000000'::uuid)::uuid AS owner_run_id,
  updated_at::timestamp
FROM absurd.get_task_checkpoint_state($1, $2, $3, $4);

-- name: GetTaskCheckpointStates :many
SELECT
  checkpoint_name::text,
  COALESCE(state, '{}')::text AS state,
  status::text,
  COALESCE(owner_run_id, '00000000-0000-0000-0000-000000000000'::uuid)::uuid AS owner_run_id,
  updated_at::timestamp
FROM absurd.get_task_checkpoint_states($1, $2, $3);

-- ----------------------------------------------------------------------------
-- Events
-- ----------------------------------------------------------------------------

-- name: AwaitEvent :one
SELECT
  should_suspend::boolean,
  COALESCE(payload, '{}')::text AS payload
FROM absurd.await_event($1, $2, $3, $4, $5, $6);

-- name: EmitEvent :exec
SELECT absurd.emit_event($1, $2, $3)::text;

-- ----------------------------------------------------------------------------
-- Claim Management
-- ----------------------------------------------------------------------------

-- name: ExtendClaim :exec
SELECT absurd.extend_claim($1, $2, $3)::text;

-- ----------------------------------------------------------------------------
-- Cleanup
-- ----------------------------------------------------------------------------

-- name: CleanupAllQueues :many
SELECT
  queue_name::text,
  tasks_deleted::integer,
  events_deleted::integer
FROM absurd.cleanup_all_queues($1);

-- name: CleanupTasks :one
SELECT cleanup_tasks::integer
FROM absurd.cleanup_tasks($1, $2, $3);

-- name: CleanupEvents :one
SELECT cleanup_events::integer
FROM absurd.cleanup_events($1, $2, $3);

-- ----------------------------------------------------------------------------
-- Utility
-- ----------------------------------------------------------------------------

-- name: GetSchemaVersion :one
SELECT get_schema_version::text
FROM absurd.get_schema_version();
