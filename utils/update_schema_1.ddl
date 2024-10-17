/******
* DDL Commands to update a schema from the initial schema to a more recent schema.
*
* This updates the tables and constraints in the 'current' schema.
* Set the current schema in postgres by setting the search path. For example,
*   set search_path=development
*
* Use this script manually in a DB IDE such as DBBeaver or SQLWorkbench/J to manually
* update a schema. You must execute this script on both the development and public schema.
*
* All schema changes must be backward compatible. You should update the development schema first
* Then run all hf_hydrodata unit tests with the environment variable export DC_SCHEMA=development
* This will verify that the new schema changes are backward compatible. After testing you can
* can update the public schema and then resume publishing changes from development to public.
*
* The update commands in this script must have the property that you can execute the script twice
* in the same schema without error. If this property is not true, then create a new file
* called update_schema_2.dll with new schema changes that assume the previous update schema files
* have been executed.
*****/
BEGIN TRANSACTION;

set search_path=development;
COMMIT;
