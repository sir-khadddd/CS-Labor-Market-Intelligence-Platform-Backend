CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS analytics.cs_job_demand (
    month DATE NOT NULL,
    geo_id TEXT NOT NULL,
    geo_name TEXT NOT NULL,
    industry_id TEXT NOT NULL,
    industry_name TEXT NOT NULL,
    role_id TEXT NOT NULL,
    role_name TEXT NOT NULL,
    posting_count BIGINT NOT NULL,
    yoy_growth DOUBLE PRECISION,
    rolling_3m_growth DOUBLE PRECISION,
    acceleration DOUBLE PRECISION,
    PRIMARY KEY (month, geo_id, industry_id, role_id)
);

CREATE TABLE IF NOT EXISTS analytics.cs_skill_demand (
    month DATE NOT NULL,
    geo_id TEXT NOT NULL,
    role_id TEXT NOT NULL,
    skill_id TEXT NOT NULL,
    skill_name TEXT NOT NULL,
    skill_posting_count BIGINT NOT NULL,
    role_posting_count BIGINT NOT NULL,
    share_within_role DOUBLE PRECISION,
    yoy_growth DOUBLE PRECISION,
    PRIMARY KEY (month, geo_id, role_id, skill_id)
);

CREATE TABLE IF NOT EXISTS analytics.role_skill_associations (
    month DATE NOT NULL,
    role_id TEXT NOT NULL,
    skill_id TEXT NOT NULL,
    co_occurrence_count BIGINT NOT NULL,
    p_skill_given_role DOUBLE PRECISION,
    lift DOUBLE PRECISION,
    PRIMARY KEY (month, role_id, skill_id)
);

CREATE TABLE IF NOT EXISTS analytics.salary_distribution (
    month DATE NOT NULL,
    geo_id TEXT NOT NULL,
    industry_id TEXT NOT NULL,
    role_id TEXT NOT NULL,
    salary_p25 DOUBLE PRECISION,
    salary_p50 DOUBLE PRECISION,
    salary_p75 DOUBLE PRECISION,
    PRIMARY KEY (month, geo_id, industry_id, role_id)
);

CREATE TABLE IF NOT EXISTS analytics.trajectory_features (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    month DATE NOT NULL,
    posting_count BIGINT NOT NULL,
    yoy_growth DOUBLE PRECISION,
    rolling_3m_growth DOUBLE PRECISION,
    acceleration DOUBLE PRECISION,
    volatility_12m DOUBLE PRECISION,
    demand_concentration_index DOUBLE PRECISION,
    momentum_score DOUBLE PRECISION,
    feature_version TEXT NOT NULL,
    run_timestamp TIMESTAMP NOT NULL,
    cs_allowlist_version TEXT NOT NULL,
    train_start_month DATE NOT NULL,
    validation_start_month DATE NOT NULL,
    PRIMARY KEY (entity_type, entity_id, month, feature_version)
);

CREATE TABLE IF NOT EXISTS analytics.trajectory_labels (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    month DATE NOT NULL,
    trajectory_class TEXT NOT NULL,
    trajectory_score DOUBLE PRECISION,
    confidence DOUBLE PRECISION,
    method TEXT NOT NULL,
    label_version TEXT NOT NULL,
    method_version TEXT NOT NULL,
    run_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (entity_type, entity_id, month, label_version, method_version)
);
