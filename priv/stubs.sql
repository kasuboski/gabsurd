-- Compatibility stubs for pg_cron so the Absurd schema can be initialized
-- without the pg_cron extension. They are not used at runtime.
CREATE SCHEMA IF NOT EXISTS cron;
CREATE TABLE IF NOT EXISTS cron.job (jobid bigint, jobname text, schedule text, command text, nodename text, nodeport integer, database text, username text, active boolean, jobclass text);
CREATE FUNCTION cron.schedule(jobname text, schedule text, command text) RETURNS bigint AS $$ BEGIN RETURN 0; END; $$ LANGUAGE plpgsql;
CREATE FUNCTION cron.unschedule(jobid bigint) RETURNS boolean AS $$ BEGIN RETURN true; END; $$ LANGUAGE plpgsql;
