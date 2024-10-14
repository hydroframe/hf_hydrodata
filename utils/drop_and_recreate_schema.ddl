BEGIN TRANSACTION;
DROP TABLE IF EXISTS version CASCADE;
CREATE TABLE version (id date, modified_by varchar(100), comments varchar(100), PRIMARY KEY (id));
DROP TABLE IF EXISTS data_catalog_entry CASCADE;
CREATE TABLE data_catalog_entry (id varchar(100), dataset varchar(100), dataset_version varchar(100), file_type varchar(100), variable varchar(100), dataset_var varchar(100), entry_start_date date, entry_end_date date, temporal_resolution varchar(100), units varchar(100), aggregation varchar(100), grid varchar(100), file_grouping varchar(100), security_level varchar(100), path varchar(1500), documentation_notes varchar(1500), site_type varchar(100), PRIMARY KEY (id));
DROP TABLE IF EXISTS dataset_version CASCADE;
CREATE TABLE dataset_version (id varchar(100), title varchar(1500), PRIMARY KEY (id));
DROP TABLE IF EXISTS aggregation CASCADE;
CREATE TABLE aggregation (id varchar(100), title varchar(1500), description varchar(1500), PRIMARY KEY (id));
DROP TABLE IF EXISTS file_type CASCADE;
CREATE TABLE file_type (id varchar(100), title varchar(1500), description varchar(1500), driver varchar(100), PRIMARY KEY (id));
DROP TABLE IF EXISTS dataset_type CASCADE;
CREATE TABLE dataset_type (id varchar(100), description varchar(1500), PRIMARY KEY (id));
DROP TABLE IF EXISTS dataset CASCADE;
CREATE TABLE dataset (id varchar(100), dataset_type varchar(100), description varchar(1500), datasource varchar(100), paper_dois varchar(100), dataset_dois varchar(100), dataset_start_date date, dataset_end_date date, structure_type varchar(100), time_zone varchar(100), documentation_link varchar(100), has_ensemble varchar(100), PRIMARY KEY (id));
DROP TABLE IF EXISTS grid CASCADE;
CREATE TABLE grid (id varchar(100), title varchar(1500), shape json, latlng_bounds json, resolution_meters varchar(100), crs_name varchar(100), origin json, crs varchar(100), PRIMARY KEY (id));
DROP TABLE IF EXISTS variable CASCADE;
CREATE TABLE variable (id varchar(100), title varchar(1500), description varchar(1500), variable_type varchar(100), unit_type varchar(100), has_z varchar(100), PRIMARY KEY (id));
DROP TABLE IF EXISTS substitution_keys CASCADE;
CREATE TABLE substitution_keys (id varchar(100), description varchar(1500), PRIMARY KEY (id));
DROP TABLE IF EXISTS structure_type CASCADE;
CREATE TABLE structure_type (id varchar(100), title varchar(1500), structure_description varchar(100), PRIMARY KEY (id));
DROP TABLE IF EXISTS unit_type CASCADE;
CREATE TABLE unit_type (id varchar(100), PRIMARY KEY (id));
DROP TABLE IF EXISTS units CASCADE;
CREATE TABLE units (id varchar(100), title varchar(1500), unit_type varchar(100), PRIMARY KEY (id));
DROP TABLE IF EXISTS variable_type CASCADE;
CREATE TABLE variable_type (id varchar(100), description varchar(1500), PRIMARY KEY (id));
DROP TABLE IF EXISTS site_type CASCADE;
CREATE TABLE site_type (id varchar(100), title varchar(1500), PRIMARY KEY (id));
DROP TABLE IF EXISTS security_level CASCADE;
CREATE TABLE security_level (id varchar(100), level_description varchar(1500), PRIMARY KEY (id));
DROP TABLE IF EXISTS temporal_resolution CASCADE;
CREATE TABLE temporal_resolution (id varchar(100), title varchar(1500), period_description varchar(100), PRIMARY KEY (id));
DROP TABLE IF EXISTS datasource CASCADE;
CREATE TABLE datasource (id varchar(100), description varchar(1500), PRIMARY KEY (id));
GRANT SELECT ON ALL TABLES IN SCHEMA public_test to "hmei-hydro"
GRANT USAGE ON SCHEMA public_test to "hmei-hydro"
GRANT SELECT ON ALL TABLES IN SCHEMA public_test to "data_catalog-rw"
GRANT USAGE ON SCHEMA public_test to "data_catalog-rw"
GRANT SELECT ON ALL TABLES IN SCHEMA public_test to "data_catalog-ro"
GRANT USAGE ON SCHEMA public_test to "data_catalog-ro"
GRANT TRUNCATE, DELETE, TRIGGER, UPDATE, REFERENCES, SELECT, INSERT  ON ALL TABLES IN SCHEMA public_test to "hmei-hydro"
GRANT ALL ON SCHEMA public_test to "hmei-hydro"
ALTER TABLE data_catalog_entry DROP CONSTRAINT IF EXISTS data_catalog_entry_dataset;
ALTER TABLE data_catalog_entry ADD CONSTRAINT data_catalog_entry_dataset FOREIGN KEY (dataset) REFERENCES dataset (id);
ALTER TABLE data_catalog_entry DROP CONSTRAINT IF EXISTS data_catalog_entry_dataset_version;
ALTER TABLE data_catalog_entry ADD CONSTRAINT data_catalog_entry_dataset_version FOREIGN KEY (dataset_version) REFERENCES dataset_version (id);
ALTER TABLE data_catalog_entry DROP CONSTRAINT IF EXISTS data_catalog_entry_file_type;
ALTER TABLE data_catalog_entry ADD CONSTRAINT data_catalog_entry_file_type FOREIGN KEY (file_type) REFERENCES file_type (id);
ALTER TABLE data_catalog_entry DROP CONSTRAINT IF EXISTS data_catalog_entry_variable;
ALTER TABLE data_catalog_entry ADD CONSTRAINT data_catalog_entry_variable FOREIGN KEY (variable) REFERENCES variable (id);
ALTER TABLE data_catalog_entry DROP CONSTRAINT IF EXISTS data_catalog_entry_temporal_resolution;
ALTER TABLE data_catalog_entry ADD CONSTRAINT data_catalog_entry_temporal_resolution FOREIGN KEY (temporal_resolution) REFERENCES temporal_resolution (id);
ALTER TABLE data_catalog_entry DROP CONSTRAINT IF EXISTS data_catalog_entry_units;
ALTER TABLE data_catalog_entry ADD CONSTRAINT data_catalog_entry_units FOREIGN KEY (units) REFERENCES units (id);
ALTER TABLE data_catalog_entry DROP CONSTRAINT IF EXISTS data_catalog_entry_aggregation;
ALTER TABLE data_catalog_entry ADD CONSTRAINT data_catalog_entry_aggregation FOREIGN KEY (aggregation) REFERENCES aggregation (id);
ALTER TABLE data_catalog_entry DROP CONSTRAINT IF EXISTS data_catalog_entry_grid;
ALTER TABLE data_catalog_entry ADD CONSTRAINT data_catalog_entry_grid FOREIGN KEY (grid) REFERENCES grid (id);
ALTER TABLE data_catalog_entry DROP CONSTRAINT IF EXISTS data_catalog_entry_security_level;
ALTER TABLE data_catalog_entry ADD CONSTRAINT data_catalog_entry_security_level FOREIGN KEY (security_level) REFERENCES security_level (id);
ALTER TABLE data_catalog_entry DROP CONSTRAINT IF EXISTS data_catalog_entry_site_type;
ALTER TABLE data_catalog_entry ADD CONSTRAINT data_catalog_entry_site_type FOREIGN KEY (site_type) REFERENCES site_type (id);
ALTER TABLE dataset DROP CONSTRAINT IF EXISTS dataset_dataset_type;
ALTER TABLE dataset ADD CONSTRAINT dataset_dataset_type FOREIGN KEY (dataset_type) REFERENCES dataset_type (id);
ALTER TABLE dataset DROP CONSTRAINT IF EXISTS dataset_datasource;
ALTER TABLE dataset ADD CONSTRAINT dataset_datasource FOREIGN KEY (datasource) REFERENCES datasource (id);
ALTER TABLE dataset DROP CONSTRAINT IF EXISTS dataset_structure_type;
ALTER TABLE dataset ADD CONSTRAINT dataset_structure_type FOREIGN KEY (structure_type) REFERENCES structure_type (id);
ALTER TABLE variable DROP CONSTRAINT IF EXISTS variable_variable_type;
ALTER TABLE variable ADD CONSTRAINT variable_variable_type FOREIGN KEY (variable_type) REFERENCES variable_type (id);
ALTER TABLE variable DROP CONSTRAINT IF EXISTS variable_unit_type;
ALTER TABLE variable ADD CONSTRAINT variable_unit_type FOREIGN KEY (unit_type) REFERENCES unit_type (id);
ALTER TABLE units DROP CONSTRAINT IF EXISTS units_unit_type;
ALTER TABLE units ADD CONSTRAINT units_unit_type FOREIGN KEY (unit_type) REFERENCES unit_type (id);
COMMIT;
