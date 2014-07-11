
-------------------------------------------------------------------------------- 
-- Purge records if all nodes on the cluster are up.
-- Set the ancient high water mark (AHM) 
-- Identify tables in a schema that have records marked for deletion
-- Purge deleted records from those tables
-- If a node is down you will need to manually delete the PurgeProjectionsFile

-- 20131210v0.1 - DH - Initial Version
-------------------------------------------------------------------------------- 
\set AUTOCOMMIT OFF
\set ON_ERROR_STOP on

-------------------------------------------------------------------------------- 
-- Set Variables
-------------------------------------------------------------------------------- 
\set SchemaName           '''public'''
\set StartDateTime        `date "+%Y%m%d%H%M%S"`
\set Program              'Purge Deleted Rows v0.1'
\set PurgeProjectionsFile     '/tmp/PurgeRecords.' :StartDateTime '.sql'
\set PurgeProjectionsFileTxt  '\'' :PurgeProjectionsFile '\''
\set 

SELECT GET_AHM_TIME(), GETDATE();

\o :PurgeProjectionsFile
\pset tuples_only

-------------------------------------------------------------------------------- 
-- Quit if a node is down.
-------------------------------------------------------------------------------- 
SELECT DISTINCT CASE WHEN node_state IS NOT NULL THEN '\q' ELSE '' END AS Action
FROM v_catalog.nodes 
WHERE node_state ILIKE 'DOWN';


-------------------------------------------------------------------------------- 
-- Throw an error (1/0) if a node is down.
-------------------------------------------------------------------------------- 
--SELECT 1/(1-COUNT(DISTINCT node_state))
--FROM v_catalog.nodes
--WHERE node_state ILIKE 'DOWN';

        
-------------------------------------------------------------------------------- 
-- Dynamically generate SQL and send the SQL to the PurgeProjectionsFiles
-------------------------------------------------------------------------------- 
SELECT 'SELECT MAKE_AHM_NOW();'
;

-------------------------------------------------------------------------------- 
-- Dynamically generate SQL and send the SQL to the PurgeProjectionsFiles
-- If you want to purge all tables then use 
-- SELECT 'SELECT PURGE();';
-------------------------------------------------------------------------------- 
SELECT 'SELECT PURGE_PROJECTION(' || '''' 
       ||  schema_name || '.' ||  projection_name || '''' 
       || ') AS ' ||  projection_name || ';'
FROM v_monitor.delete_vectors
WHERE schema_name IN ( :SchemaName )
; 

-------------------------------------------------------------------------------- 
-- Remove the dynamic SQL file when done. 
-- Change the rm to a cat when testing to prevent the file from being deleted.
-------------------------------------------------------------------------------- 
SELECT '\! rm ' || :PurgeProjectionsFileTxt as RemoveTempFile 
;

\o
\pset tuples_only

-------------------------------------------------------------------------------- 
-- Set AHM, purge projections, and remove the temp file
-------------------------------------------------------------------------------- 
\i :PurgeProjectionsFile 

SELECT GET_AHM_TIME(), GETDATE();

\q
