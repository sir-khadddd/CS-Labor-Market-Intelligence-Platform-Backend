CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id TEXT PRIMARY KEY,
    run_timestamp TIMESTAMP NOT NULL,
    source_start_month DATE,
    source_end_month DATE,
    feature_version TEXT NOT NULL,
    label_version TEXT NOT NULL,
    method_version TEXT NOT NULL,
    cs_allowlist_version TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS metadata.source_lineage (
    run_id TEXT NOT NULL REFERENCES metadata.pipeline_runs(run_id),
    source_name TEXT NOT NULL,
    source_path TEXT NOT NULL,
    row_count BIGINT,
    checksum TEXT,
    PRIMARY KEY (run_id, source_name, source_path)
);

CREATE TABLE IF NOT EXISTS metadata.model_eval_splits (
    split_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES metadata.pipeline_runs(run_id),
    entity_type TEXT NOT NULL,
    train_start_month DATE NOT NULL,
    train_end_month DATE NOT NULL,
    validation_start_month DATE NOT NULL,
    validation_end_month DATE,
    test_start_month DATE,
    test_end_month DATE
);
