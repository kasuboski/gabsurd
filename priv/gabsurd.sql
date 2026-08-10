-- Local SQL extensions for gabsurd. Keep priv/absurd.sql untouched.
--
-- Exposes task routing metadata alongside Absurd's task result without exposing
-- task params. The dynamic table name is validated and quoted before use.
create schema if not exists gabsurd;

create or replace function gabsurd.get_task_result (
  p_queue_name text,
  p_task_id uuid
)
  returns table (
    task_id uuid,
    task_name text,
    headers jsonb,
    state text,
    result jsonb,
    failure_reason jsonb
  )
  language plpgsql
as $$
declare
  v_queue_name text := absurd.validate_queue_name(p_queue_name);
begin
  return query execute format(
    'with upstream_result as (
       select * from absurd.get_task_result($1, $2)
     )
     select r.task_id,
            t.task_name,
            coalesce(t.headers, ''{}''::jsonb),
            r.state,
            r.result,
            r.failure_reason
       from absurd.%I t
       join upstream_result r on r.task_id = t.task_id
      where t.task_id = $2',
    't_' || v_queue_name
  ) using v_queue_name, p_task_id;
end;
$$;
