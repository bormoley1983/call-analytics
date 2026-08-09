# Schema Migrations
# ================
#
# Versioned DDL migrations for the call-analytics Postgres schema.
#
# ## How it works
#
# 1. Migration versions are tracked in `schema_migrations` table
# 2. On connection init (in `postgres_single_connection.py`), only unapplied migrations are executed
# 3. Each version file contains idempotent SQL (CREATE IF NOT EXISTS, etc.)
# 4. Once applied, the version is recorded and subsequent connections skip all DDL
#
# ## Benefits over the old approach
#
# - **No repetitive DDL**: Old approach ran ALL CREATE TABLE/ALTER TABLE on every connection
# - **No lock deadlocks**: `CREATE TABLE IF NOT EXISTS` requires ACCESS EXCLUSIVE locks;
#   with migrations, we only run DDL once per version, then skip entirely
# - **Fast connections**: Schema check is a simple SELECT from schema_migrations (~1ms)
# - **Traceable**: Each migration has a version and timestamp in the tracking table
#
# ## Adding a new migration
#
# 1. Create a new file: `V<NNN>__<description>.sql`
#    - Version is monotonically increasing 3-digit integer (e.g., V003)
#    - Description is lowercase with underscores (e.g., V003__add_phone_filters.sql)
# 2. The file should contain ALL SQL statements for that version
# 3. Add the version to `ALL_VERSIONS` list in `__init__.py`
#
# ## Current versions
#
# | Version | Description | Tables/Columns |
# |---------|-------------|----------------|
# | V001   | Core schema (all tables + indexes) | transcripts, analyses, calls, keywords, call_keywords, ai_apply, ai_alias_suggestions, ai_deep_insights, etc. |
# | V002   | STT runs schema | stt_runs, stt_results, transcript/analysis columns for STT provenance |
