-----------------
-- API Keys    --
-----------------
CREATE TABLE api_keys (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR NOT NULL,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    name TEXT NOT NULL,
    api_key TEXT NOT NULL
);

-----------------
-- Definitions --
-----------------
DROP TYPE IF EXISTS connection_type;
create TYPE connection_type as ENUM ('kafka', 'kinesis');

CREATE TABLE connections (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ,
    updated_by VARCHAR,
    deleted_at TIMESTAMPTZ,
    name TEXT NOT NULL,
    type connection_type NOT NULL,
    config JSONB NOT NULL,
    UNIQUE (organization_id, name)
);

DROP TYPE IF EXISTS schema_type;
create TYPE schema_type as ENUM ('builtin', 'json_schema', 'json_fields');

CREATE TABLE schemas (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    name TEXT NOT NULL,
    kafka_schema_registry boolean NOT NULL,
    type schema_type NOT NULL,
    config JSONB,

    UNIQUE (organization_id, name)
);

DROP TYPE IF EXISTS source_type;
create TYPE source_type as ENUM ('kafka', 'impulse', 'file', 'nexmark');

CREATE TABLE sources (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    name TEXT NOT NULL,

    type source_type NOT NULL,
    connection_id BIGINT REFERENCES connections(id),
    config JSONB,

    schema_id BIGINT REFERENCES schemas(id) NOT NULL,

    UNIQUE (organization_id, name)
);

DROP TYPE IF EXISTS sink_type;
CREATE TYPE sink_type as ENUM ('kafka', 'state');


CREATE TABLE sinks (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    name TEXT NOT NULL,
    type sink_type NOT NULL,
    connection_id BIGINT REFERENCES connections(id),
    config JSONB,

    UNIQUE (organization_id, name)
);

DROP TYPE IF EXISTS pipeline_type;
CREATE TYPE pipeline_type as ENUM ('sql', 'rust');

CREATE TABLE pipelines (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    name TEXT NOT NULL,
    type pipeline_type NOT NULL,
    current_version INT NOT NULL
);

CREATE TABLE pipeline_definitions (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,

    pipeline_id BIGINT REFERENCES pipelines ON DELETE CASCADE,
    version INT NOT NULL,
    textual_repr TEXT,
    program BYTEA NOT NULL,

    UNIQUE (pipeline_id, version)
);


-- NOTE: preview pipelines are not added to these tables
CREATE TABLE pipeline_sources (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGSERIAL REFERENCES pipeline_definitions(id) ON DELETE CASCADE,
    source_id BIGSERIAL REFERENCES sources(id)
);

CREATE TABLE pipeline_sinks (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGSERIAL REFERENCES pipeline_definitions(id) ON DELETE CASCADE,
    sink_id BIGSERIAL REFERENCES sinks(id)
);


------------------------------
-- Controller state machine --
------------------------------
DROP TYPE IF EXISTS stop_mode;
CREATE TYPE stop_mode as ENUM ('none', 'checkpoint', 'graceful', 'immediate', 'force');

CREATE TABLE job_configs (
    id VARCHAR(8) PRIMARY KEY,
    organization_id VARCHAR,

    -- denormalized from pipelines to save a join
    pipeline_name TEXT NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,

    ttl_micros BIGINT,

    stop stop_mode DEFAULT 'none',

    pipeline_definition BIGINT REFERENCES pipeline_definitions(id) ON DELETE CASCADE NOT NULL,

    parallelism_overrides JSONB default '{}',

    checkpoint_interval_micros BIGINT NOT NULL DEFAULT 10000000
);

DROP TYPE IF EXISTS checkpoint_state;
create type checkpoint_state as ENUM ('inprogress', 'ready', 'compacting', 'compacted', 'failed');

CREATE TABLE checkpoints (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR,
    job_id VARCHAR(8) REFERENCES job_configs(id) ON DELETE CASCADE NOT NULL,

    state_backend TEXT NOT NULL,
    epoch INT NOT NULL,
    min_epoch INT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    finish_time TIMESTAMPTZ,
    state checkpoint_state DEFAULT 'inprogress',

    operators JSONB DEFAULT '{}' NOT NULL
);

CREATE INDEX checkpoints_job_id_idx ON checkpoints (job_id);


CREATE TABLE job_statuses (
    id VARCHAR(8) PRIMARY KEY REFERENCES job_configs(id) ON DELETE CASCADE,
    organization_id VARCHAR,
    controller_id BIGINT,

    --! identifies this particular run of the pipeline
    run_id BIGINT NOT NULL DEFAULT 0,

    start_time TIMESTAMPTZ,
    finish_time TIMESTAMPTZ,

    tasks INT,

    failure_message TEXT,
    restarts INT DEFAULT 0,

    pipeline_path VARCHAR,
    wasm_path VARCHAR,

    state TEXT DEFAULT 'Created'
);
ALTER TYPE schema_type ADD VALUE 'raw_json';
ALTER TABLE pipeline_definitions
ADD COLUMN udfs JSONB NOT NULL DEFAULT '[]';ALTER TYPE source_type
ADD VALUE 'event_source';
ALTER TYPE connection_type
ADD VALUE 'http';
CREATE TABLE cluster_info (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    singleton INT NOT NULL DEFAULT 0 UNIQUE CHECK (singleton = 0)
);

INSERT INTO cluster_info (id, name) VALUES (gen_random_uuid(), 'default');
DROP TYPE IF EXISTS log_level;
create type log_level as ENUM ('info', 'warn', 'error');

CREATE TABLE job_log_messages (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    job_id VARCHAR(8) REFERENCES job_configs(id) ON DELETE CASCADE,
    operator_id TEXT,
    task_index BIGINT,
    log_level log_level DEFAULT 'info',
    message TEXT NOT NULL,
    details TEXT
);
ALTER TABLE connections
ALTER COLUMN type TYPE TEXT using type::text;

CREATE TABLE connection_tables (
    id BIGSERIAL PRIMARY KEY,
    organization_id VARCHAR NOT NULL,
    created_by VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_by VARCHAR,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,

    name TEXT NOT NULL,
    table_type TEXT NOT NULL, -- currently one of source or sink
    connector TEXT NOT NULL,
    connection_id BIGINT REFERENCES connections(id),
    config JSONB,
    -- json-serialized ConnectionSchema
    schema JSONB,

    UNIQUE (organization_id, name)
);

CREATE TABLE connection_table_pipelines (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGSERIAL REFERENCES pipeline_definitions(id) ON DELETE CASCADE,
    connection_table_id BIGSERIAL REFERENCES connection_tables(id)
);

DROP TABLE pipeline_sinks;
DROP TABLE pipeline_sources;
DROP TABLE sinks;
DROP TABLE sources;
ALTER TABLE api_keys
ADD COLUMN pub_id VARCHAR UNIQUE;

ALTER TABLE connections
ADD COLUMN pub_id VARCHAR UNIQUE;

ALTER TABLE schemas
ADD COLUMN pub_id VARCHAR UNIQUE;

ALTER TABLE pipelines
ADD COLUMN pub_id VARCHAR UNIQUE;

ALTER TABLE pipeline_definitions
ADD COLUMN pub_id VARCHAR UNIQUE;

ALTER TABLE job_configs
ADD COLUMN pub_id VARCHAR UNIQUE;

ALTER TABLE checkpoints
ADD COLUMN pub_id VARCHAR UNIQUE;

ALTER TABLE job_statuses
ADD COLUMN pub_id VARCHAR UNIQUE;

ALTER TABLE cluster_info
ADD COLUMN pub_id VARCHAR UNIQUE;

ALTER TABLE job_log_messages
ADD COLUMN pub_id VARCHAR UNIQUE;

ALTER TABLE connection_tables
ADD COLUMN pub_id VARCHAR UNIQUE;

ALTER TABLE connection_table_pipelines
ADD COLUMN pub_id VARCHAR UNIQUE;
-- We have two tables, pipelines and pipeline_definitions, that are
-- conceptually one table, and which generally have a 1-1 relationship.
-- pipeline_definitions stores the history of pipeline definitions, and
-- pipelines references the current version.
--
-- This distinction has not proved useful, and so this migration is
-- intended to merge the two tables into one. At the end of this,

ALTER TABLE pipelines ADD COLUMN textual_repr TEXT;
ALTER TABLE pipelines ADD COLUMN program BYTEA;
ALTER TABLE pipelines ADD COLUMN udfs JSONB NOT NULL DEFAULT '[]';

-- fill in the new fields on pipelines
UPDATE pipelines
SET textual_repr = pipeline_definitions.textual_repr,
    program = pipeline_definitions.program,
    udfs = pipeline_definitions.udfs
FROM pipeline_definitions
WHERE pipelines.id = pipeline_definitions.pipeline_id
AND pipelines.current_version = pipeline_definitions.version;

ALTER TABLE pipelines DROP COLUMN current_version;
ALTER TABLE pipelines ALTER COLUMN textual_repr SET NOT NULL;
ALTER TABLE pipelines ALTER COLUMN program SET NOT NULL;

-- modify connection_table_pipelines so that it references the pipeline, instead of pipeline_definition
ALTER TABLE connection_table_pipelines RENAME COLUMN pipeline_id to pipeline_definition_id;
ALTER TABLE connection_table_pipelines ADD COLUMN pipeline_id BIGINT REFERENCES pipelines(id);

UPDATE connection_table_pipelines
SET pipeline_id = pipeline_definition_id;

ALTER TABLE connection_table_pipelines ALTER COLUMN pipeline_id SET NOT NULL;
ALTER TABLE connection_table_pipelines DROP COLUMN pipeline_definition_id;

-- modify job_configs to reference the pipeline instead of pipeline_definition
ALTER TABLE job_configs ADD COLUMN pipeline_id BIGINT REFERENCES pipelines(id);

UPDATE job_configs
SET pipeline_id = pipeline_definition;

ALTER TABLE job_configs ALTER COLUMN pipeline_id SET NOT NULL;
ALTER TABLE job_configs DROP COLUMN pipeline_definition;

-- drop pipeline_Definitions
DROP TABLE pipeline_definitions;
-- add missing ON DELETE CASCADE to connection_table_pipelines
ALTER TABLE connection_table_pipelines RENAME COLUMN pipeline_id to pipeline_id_old;
ALTER TABLE connection_table_pipelines
    ADD COLUMN pipeline_id BIGINT REFERENCES pipelines(id) ON DELETE CASCADE;
UPDATE connection_table_pipelines SET pipeline_id = pipeline_id_old;
ALTER TABLE connection_table_pipelines DROP COLUMN pipeline_id_old;

-- add missing ON DELETE CASCADE to job_configs
ALTER TABLE job_configs RENAME COLUMN pipeline_id to pipeline_id_old;
ALTER TABLE job_configs
    ADD COLUMN pipeline_id BIGINT REFERENCES pipelines(id) ON DELETE CASCADE;
UPDATE job_configs SET pipeline_id = pipeline_id_old;
ALTER TABLE job_configs DROP COLUMN pipeline_id_old;
ALTER TYPE checkpoint_state ADD VALUE 'committing';
UPDATE api_keys
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE api_keys
ALTER COLUMN pub_id SET NOT NULL;

UPDATE connections
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE connections
ALTER COLUMN pub_id SET NOT NULL;

UPDATE schemas
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE schemas
ALTER COLUMN pub_id SET NOT NULL;

UPDATE pipelines
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE pipelines
ALTER COLUMN pub_id SET NOT NULL;

UPDATE job_configs
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE job_configs
ALTER COLUMN pub_id SET NOT NULL;

UPDATE checkpoints
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE checkpoints
ALTER COLUMN pub_id SET NOT NULL;

UPDATE job_statuses
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE job_statuses
ALTER COLUMN pub_id SET NOT NULL;

UPDATE cluster_info
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE cluster_info
ALTER COLUMN pub_id SET NOT NULL;

UPDATE job_log_messages
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE job_log_messages
ALTER COLUMN pub_id SET NOT NULL;

UPDATE connection_tables
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE connection_tables
ALTER COLUMN pub_id SET NOT NULL;

UPDATE connection_table_pipelines
SET pub_id = COALESCE(pub_id, CAST(id as VARCHAR));

ALTER TABLE connection_table_pipelines
ALTER COLUMN pub_id SET NOT NULL;
-- going forward the 'id' column will be used to store the public id

ALTER TABLE job_configs
DROP COLUMN pub_id;

ALTER TABLE job_configs
ALTER COLUMN id TYPE VARCHAR;

ALTER TABLE job_configs
ADD CONSTRAINT job_configs_unique_id UNIQUE (id);

ALTER TABLE checkpoints
ALTER COLUMN job_id TYPE VARCHAR;

ALTER TABLE job_statuses
ALTER COLUMN id TYPE VARCHAR;

ALTER TABLE job_log_messages
ALTER COLUMN job_id TYPE VARCHAR;
ALTER TABLE connections RENAME TO connection_profiles;
