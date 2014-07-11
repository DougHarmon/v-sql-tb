
-------------------------------------------------------------------------------- 
-- Identify tables in a schema that have 
--  1. No Statistics
--  2. Statistics more than 1 day out of date 
--     i.e. (Modify Date - 1 Day) > Date Statistics Last Updated 
--  3. Contain data
-- 
-- Collect full statistics on the tables identified above.
-- 20131209v0.1 - DH - Initial Version
-------------------------------------------------------------------------------- 
\set AUTOCOMMIT OFF
\set ON_ERROR_STOP on

-------------------------------------------------------------------------------- 
-- Set Variables
-------------------------------------------------------------------------------- 
\set SchemaName           '''public'''
\set StartDateTime        `date "+%Y%m%d%H%M%S"`
\set Program              'Update Statistics v1'
\set AnalyzeStatsFile     '/tmp/analyze_statistics.' :StartDateTime '.sql'
\set AnalyzeStatsFileTxt  '\'' :AnalyzeStatsFile '\''
\set

SELECT GETDATE();

\o :AnalyzeStatsFile
\pset tuples_only

-------------------------------------------------------------------------------- 
-- Dynamically generate SQL and send the SQL to the AnalyzeStatsFiles
-------------------------------------------------------------------------------- 
SELECT 'SELECT ANALYZE_STATISTICS(' || '''' 
       || T1.table_schema || '.' || T1.table_name || '''' 
       || ') AS ' || T1.table_name || ';'
FROM (
    -- Tables with no statistics
    SELECT table_id
        ,table_schema
        ,table_name
    FROM v_catalog.projection_columns
    WHERE statistics_type NOT LIKE 'FULL'
        AND table_schema ILIKE :SchemaName
    GROUP BY table_id
        ,table_schema
        ,table_name
    
    UNION
    
    -- Statistics more than 1 day out of date.
    SELECT S.table_id
        ,S.table_schema
        ,S.table_name
    FROM (
        -- Tables which have full statistics
        SELECT table_id
            ,table_schema
            ,table_name
            ,MIN(statistics_updated_timestamp)::DATE AS full_statistics_last_updated
        FROM v_catalog.projection_columns
        WHERE statistics_type ILIKE 'FULL'
            AND table_schema ILIKE :SchemaName
        GROUP BY table_id
            ,table_schema
            ,table_name
        ) AS S
    LEFT JOIN (
        -- Tables which have been recently modified
        SELECT anchor_table_id AS table_id
            ,MAX(query_start_timestamp)::DATE AS last_modified
        FROM v_monitor.projection_usage
        WHERE io_type ILIKE 'output'
            AND anchor_table_schema ILIKE :SchemaName
        GROUP BY anchor_table_id
        ) M
        ON S.table_id = M.table_id
    WHERE (M.last_modified - 1) > S.full_statistics_last_updated
    ) AS T1
JOIN ( -- Filter out tables with no records
       SELECT anchor_table_id as table_id
           FROM v_monitor.projection_storage
          WHERE ros_count>0 OR wos_used_bytes>0    
       GROUP BY anchor_table_id
     ) as T2 
ON T1.table_id=T2.table_id  
;

-------------------------------------------------------------------------------- 
-- Remove the dynamic SQL file when done. 
-- Change the rm to a cat when testing to prevent the file from being deleted.
-------------------------------------------------------------------------------- 
SELECT '\! rm ' || :AnalyzeStatsFileTxt as RemoveTempFile ;

\o
\pset tuples_only

-------------------------------------------------------------------------------- 
-- Analyze Statistics and remove the temp file
-------------------------------------------------------------------------------- 
\i :AnalyzeStatsFile 

SELECT GETDATE();

\q
