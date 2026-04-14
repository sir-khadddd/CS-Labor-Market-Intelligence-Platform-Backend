CREATE INDEX IF NOT EXISTS idx_job_demand_role_month
ON analytics.cs_job_demand (role_id, month DESC);

CREATE INDEX IF NOT EXISTS idx_job_demand_industry_month
ON analytics.cs_job_demand (industry_id, month DESC);

CREATE INDEX IF NOT EXISTS idx_skill_demand_skill_month
ON analytics.cs_skill_demand (skill_id, month DESC);

CREATE INDEX IF NOT EXISTS idx_role_skill_lift_month
ON analytics.role_skill_associations (month DESC, lift DESC);

CREATE INDEX IF NOT EXISTS idx_salary_role_geo_month
ON analytics.salary_distribution (role_id, geo_id, month DESC);

CREATE INDEX IF NOT EXISTS idx_traj_labels_entity_month
ON analytics.trajectory_labels (entity_type, entity_id, month DESC);
