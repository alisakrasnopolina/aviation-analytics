-- 012: каталог, lineage и журнал общих запусков. История Gold сюда не входит.
CREATE TABLE IF NOT EXISTS meta.catalog_versions (
 catalog_version TEXT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), snapshot JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS meta.dataset_catalog (
 dataset_id TEXT PRIMARY KEY,name TEXT NOT NULL,layer TEXT NOT NULL,kind TEXT NOT NULL,
 location TEXT NOT NULL,grain TEXT NOT NULL,description TEXT NOT NULL,
 catalog_version TEXT NOT NULL REFERENCES meta.catalog_versions(catalog_version)
);
CREATE TABLE IF NOT EXISTS meta.transformation_catalog (
 transformation_id TEXT PRIMARY KEY,name TEXT NOT NULL,file_path TEXT NOT NULL,
 code_sha256 TEXT NOT NULL,description TEXT NOT NULL,
 catalog_version TEXT NOT NULL REFERENCES meta.catalog_versions(catalog_version)
);
CREATE TABLE IF NOT EXISTS meta.metric_catalog (
 metric_id TEXT PRIMARY KEY,name TEXT NOT NULL,dataset_id TEXT NOT NULL REFERENCES meta.dataset_catalog(dataset_id),
 formula TEXT NOT NULL,unit TEXT NOT NULL,source_fields JSONB NOT NULL,limitations TEXT NOT NULL,
 catalog_version TEXT NOT NULL REFERENCES meta.catalog_versions(catalog_version)
);
CREATE TABLE IF NOT EXISTS meta.lineage_edges (
 source_id TEXT NOT NULL REFERENCES meta.dataset_catalog(dataset_id),
 target_id TEXT NOT NULL REFERENCES meta.dataset_catalog(dataset_id),
 transformation_id TEXT NOT NULL REFERENCES meta.transformation_catalog(transformation_id),
 catalog_version TEXT NOT NULL REFERENCES meta.catalog_versions(catalog_version),
 CHECK (source_id<>target_id),PRIMARY KEY(source_id,target_id,transformation_id)
);
CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
 run_id TEXT PRIMARY KEY,name TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('RUNNING','SUCCESS','FAILED')),
 started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),finished_at TIMESTAMPTZ,is_demo BOOLEAN NOT NULL DEFAULT FALSE,
 catalog_version TEXT NOT NULL REFERENCES meta.catalog_versions(catalog_version),error_type TEXT
);
CREATE TABLE IF NOT EXISTS meta.pipeline_steps (
 step_id TEXT PRIMARY KEY,run_id TEXT NOT NULL REFERENCES meta.pipeline_runs(run_id),
 step_no INTEGER NOT NULL,transformation_id TEXT NOT NULL REFERENCES meta.transformation_catalog(transformation_id),
 status TEXT NOT NULL CHECK(status IN ('RUNNING','SUCCESS','FAILED')),started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 finished_at TIMESTAMPTZ,duration_ms DOUBLE PRECISION,input_datasets JSONB NOT NULL,output_datasets JSONB NOT NULL,
 code_file TEXT NOT NULL,code_sha256 TEXT NOT NULL,error_type TEXT,
 UNIQUE(run_id,step_no)
);
CREATE TABLE IF NOT EXISTS meta.pipeline_references (
 step_id TEXT NOT NULL REFERENCES meta.pipeline_steps(step_id),reference_type TEXT NOT NULL,
 reference_id TEXT NOT NULL,PRIMARY KEY(step_id,reference_type,reference_id)
);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_started ON meta.pipeline_runs(started_at DESC);
CREATE OR REPLACE VIEW gold.metadata_datasets_bi AS SELECT * FROM meta.dataset_catalog;
CREATE OR REPLACE VIEW gold.metadata_metrics_bi AS SELECT * FROM meta.metric_catalog;
CREATE OR REPLACE VIEW gold.metadata_lineage_bi AS
 SELECT e.source_id,s.name source_name,s.location source_location,e.target_id,t.name target_name,
 t.location target_location,e.transformation_id,x.file_path,x.code_sha256,e.catalog_version
 FROM meta.lineage_edges e JOIN meta.dataset_catalog s ON s.dataset_id=e.source_id
 JOIN meta.dataset_catalog t ON t.dataset_id=e.target_id
 JOIN meta.transformation_catalog x USING(transformation_id);
CREATE OR REPLACE VIEW gold.pipeline_execution_runs_bi AS
 SELECT * FROM meta.pipeline_runs WHERE NOT is_demo;
CREATE OR REPLACE VIEW gold.pipeline_execution_steps_bi AS
 SELECT s.*,r.name pipeline_name,r.status pipeline_status,r.catalog_version
 FROM meta.pipeline_steps s JOIN meta.pipeline_runs r USING(run_id) WHERE NOT r.is_demo;
