--==============================================================================
-- Diagnostic Views
--
-- vProjectionUsage 
-- vProjectionColumnSize  
-- vRunningQueries
-- vLongRunningQueries
-- vQueryPlanProfiles
-- vMostCommonQueries
--==============================================================================


--=========================================================================
--Create vProjectionUsage 
--=========================================================================
DROP VIEW IF EXISTS vstb.vProjectionUsage ;

CREATE VIEW vstb.vProjectionUsage 
AS
SELECT /*+ label(vProjectionUsage) */
     PS.table_schema
    ,PS.table_name
    ,PS.table_schema || '.' || PS.table_name AS table_schema_name
    ,P.projection_basename
    ,P.projection_schema
    ,P.projection_name
    ,P.projection_schema || '.' || P.projection_name AS projection_schema_name       
    ,PS.node_name
    ,right(PS.node_name,4)::INT as node_number
    ,P.owner_name
    ,P.create_type
    ,P.verified_fault_tolerance
    ,CASE WHEN P.is_prejoin           = 1 THEN 'TRUE' ELSE '***FALSE***' END AS is_prejoin          
    ,CASE WHEN P.is_up_to_date        = 1 THEN 'TRUE' ELSE '***FALSE***' END AS is_up_to_date       
    ,CASE WHEN P.has_statistics       = 1 THEN 'TRUE' ELSE '***FALSE***' END AS has_statistics      
    ,CASE WHEN P.is_segmented         = 1 THEN 'TRUE' ELSE '***FALSE***' END AS is_segmented        
    ,CASE WHEN P.is_super_projection  = 1 THEN 'TRUE' ELSE '***FALSE***' END AS is_super_projection 
    ,PC.full_statistics_last_updated
    ,DATEDIFF('DAY',PC.full_statistics_last_updated, GETDATE()) AS days_since_full_statistics_last_updated
    ,PS.projection_column_count
    ,PS.row_count
    ,SUM(PS.row_count) OVER (PARTITION BY P.projection_basename) as projection_row_count
    ,PS.row_count / ( SUM(PS.row_count) OVER (PARTITION BY P.projection_basename) ) as pct_of_rows_in_projection
    ,  (PS.row_count/ (SUM(PS.row_count) OVER (PARTITION BY P.projection_basename) / SUM(1) OVER (PARTITION BY P.projection_basename)))
       AS indexed_projection_skew
    ,PS.used_bytes
    ,PS.used_bytes/1E9  AS used_GB
    ,PS.wos_row_count
    ,PS.wos_used_bytes
    ,PS.ros_row_count
    ,PS.ros_used_bytes
    ,PS.ros_count
    ,COALESCE(D.deleted_row_count ,0)     AS deleted_row_count
    ,COALESCE(D.deleted_used_bytes,0)     AS deleted_used_bytes
    ,COALESCE(D.deleted_used_bytes,0)/1E9 AS deleted_used_GB     
    ,T.TableUniqueExtractUsers
    ,T.TableExtractTransactions
    ,T.TableDaysSinceLastExtract
    ,T.TableLastExtractDT
    ,T.TableFirstExtractDT
    ,T.TableDaysSinceLastModify
    ,T.TableLastModifyDT
    ,T.TableFirstModifyDT
    ,PU.ProjectionUniqueExtractUsers
    ,PU.ProjectionExtractTransactions
    ,PU.ProjectionDaysSinceLastExtract
    ,PU.ProjectionLastExtractDT
    ,PU.ProjectionFirstExtractDT
    ,PU.ProjectionDaysSinceLastModify
    ,PU.ProjectionLastModifyDT
    ,PU.ProjectionFirstModifyDT
FROM v_catalog.projections AS P
LEFT JOIN 
    (SELECT  
         node_name
        ,projection_id        
        ,projection_name
        ,projection_schema
        ,projection_column_count
        ,row_count
        ,used_bytes
        ,wos_row_count
        ,wos_used_bytes
        ,ros_row_count
        ,ros_used_bytes
        ,ros_count
        ,anchor_table_name    AS table_name
        ,anchor_table_schema  AS table_schema
        ,anchor_table_id      AS table_id
    FROM v_monitor.projection_storage   ) AS PS
ON P.projection_id=PS.projection_id    
LEFT JOIN 
   (SELECT   
             table_id
            ,COUNT(DISTINCT CASE WHEN COALESCE(IsExtract,0)=1 THEN user_name ELSE NULL END)  AS TableUniqueExtractUsers
            ,COUNT(NULLIFZERO(IsExtract)*transaction_id)   AS TableExtractTransactions
            ,MIN(DaysSinceLastExtract)                     AS TableDaysSinceLastExtract
            ,MIN(DaysSinceLastModify)                      AS TableDaysSinceLastModify
            ,MAX(ExtractDT)                                AS TableLastExtractDT
            ,MAX(ModifyDT)                                 AS TableLastModifyDT
            ,MIN(ExtractDT)                                AS TableFirstExtractDT
            ,MIN(ModifyDT)                                 AS TableFirstModifyDT
    FROM
         (SELECT
             transaction_id
            ,user_name
            ,anchor_table_id  as table_id
            ,projection_id            
            ,CASE WHEN io_type LIKE 'input'  THEN query_start_timestamp::DATE ELSE NULL END AS ExtractDT
            ,CASE WHEN io_type LIKE 'output' THEN query_start_timestamp::DATE ELSE NULL END AS ModifyDT
            ,CASE WHEN io_type LIKE 'input'  THEN 1 ELSE 0 END AS IsExtract
            ,CASE WHEN io_type LIKE 'output' THEN 1 ELSE 0 END AS IsModify
            ,DATEDIFF('DAY'
                     ,CASE WHEN io_type LIKE 'input'   THEN query_start_timestamp::DATE ELSE NULL END
                     ,GETDATE()) AS DaysSinceLastExtract
            ,DATEDIFF('DAY'
                     ,CASE WHEN io_type LIKE 'output'  THEN query_start_timestamp::DATE ELSE NULL END
                     ,GETDATE()) AS DaysSinceLastModify
         FROM v_monitor.projection_usage ) AS B1
    GROUP BY table_id) AS T
ON  PS.table_id  = T.table_id 
LEFT JOIN 
   (SELECT   
             projection_id
            ,COUNT(DISTINCT CASE WHEN COALESCE(IsExtract,0)=1 THEN user_name ELSE NULL END)  AS  ProjectionUniqueExtractUsers
            ,COUNT(NULLIFZERO(IsExtract)*transaction_id)   AS ProjectionExtractTransactions
            ,MIN(DaysSinceLastExtract)                     AS ProjectionDaysSinceLastExtract
            ,MIN(DaysSinceLastModify)                      AS ProjectionDaysSinceLastModify
            ,MAX(ExtractDT)                                AS ProjectionLastExtractDT
            ,MAX(ModifyDT)                                 AS ProjectionLastModifyDT
            ,MIN(ExtractDT)                                AS ProjectionFirstExtractDT
            ,MIN(ModifyDT)                                 AS ProjectionFirstModifyDT
    FROM
         (SELECT
             transaction_id
            ,user_name
            ,anchor_table_id  as table_id
            ,projection_id            
            ,CASE WHEN io_type LIKE 'input'  THEN query_start_timestamp::DATE ELSE NULL END AS ExtractDT
            ,CASE WHEN io_type LIKE 'output' THEN query_start_timestamp::DATE ELSE NULL END AS ModifyDT
            ,CASE WHEN io_type LIKE 'input'  THEN 1 ELSE 0 END AS IsExtract
            ,CASE WHEN io_type LIKE 'output' THEN 1 ELSE 0 END AS IsModify
            ,DATEDIFF('DAY'
                     ,CASE WHEN io_type LIKE 'input'   THEN query_start_timestamp::DATE ELSE NULL END
                     ,GETDATE()) AS DaysSinceLastExtract
            ,DATEDIFF('DAY'
                     ,CASE WHEN io_type LIKE 'output'  THEN query_start_timestamp::DATE ELSE NULL END
                     ,GETDATE()) AS DaysSinceLastModify
         FROM v_monitor.projection_usage ) AS B1
    GROUP BY projection_id) AS PU
   ON  P.projection_id = PU.projection_id
LEFT JOIN 
    (SELECT 
        schema_name            AS projection_schema
       ,projection_name
       ,SUM(deleted_row_count) AS deleted_row_count
       ,SUM(used_bytes)        AS deleted_used_bytes
    FROM v_monitor.delete_vectors
    GROUP BY 
        schema_name
       ,projection_name ) AS D    
ON  P.projection_schema=D.projection_schema
AND P.projection_name=D.projection_name   
LEFT JOIN 
    (SELECT projection_id
           ,MIN(statistics_updated_timestamp)::DATE AS full_statistics_last_updated
       FROM v_catalog.projection_columns     
      WHERE statistics_type LIKE 'FULL'
    GROUP BY projection_id) AS PC 
  ON P.projection_id = PC.projection_id
ORDER BY
     PS.table_schema
    ,PS.table_name
    ,PS.projection_schema
    ,PS.projection_name
    ,P.projection_basename
    ,PS.node_name
;


 

--=========================================================================
--Create vProjectionColumnSize
------ Courtesy of Eli Reiman
--=========================================================================
DROP VIEW IF EXISTS vstb.vProjectionColumnSize ;

CREATE VIEW vstb.vProjectionColumnSize  
AS
 SELECT 
    tab.anchor_table_schema
  , tab.anchor_table_name
  , tab.GB_Table
  , proj.projection_name
  , proj.GB_Proj
  , col.column_name
  , col.MB_Col
  , col.row_count
  , col.encodings
  , col.compressions
  , colOrder.column_position
  , colOrder.sort_position
FROM 
    ( SELECT anchor_table_schema
          , anchor_table_name
          , ((SUM(used_bytes)/1E9::FLOAT))::INT AS GB_Table
       FROM v_monitor.projection_storage
   GROUP BY anchor_table_schema
          , anchor_table_name ) tab
LEFT JOIN
    ( SELECT anchor_table_schema
          , anchor_table_name
          , projection_name
          , ((SUM(used_bytes)/1E9::FLOAT))::INT AS GB_Proj
       FROM v_monitor.projection_storage
   GROUP BY anchor_table_schema
          , anchor_table_name
          , projection_name ) proj
 ON proj.anchor_table_schema = tab.anchor_table_schema  
AND  proj.anchor_table_name = tab.anchor_table_name  
LEFT JOIN
    ( SELECT anchor_table_schema
          , anchor_table_name
          , projection_name
          , column_name
          , encodings
          , compressions
          , ((SUM(used_bytes)/1E6::FLOAT))::INT AS MB_Col
          , SUM(row_count)                      AS row_count
       FROM v_monitor.column_storage
   GROUP BY anchor_table_schema
          , anchor_table_name
          , projection_name
          , column_name
          , encodings
          , compressions ) col
 ON col.anchor_table_schema = tab.anchor_table_schema  
AND   col.anchor_table_name = tab.anchor_table_name  
AND  col.projection_name = proj.projection_name  
LEFT JOIN v_catalog.projection_columns colOrder
 ON  colOrder.table_schema = tab.anchor_table_schema  
AND colOrder.table_name = tab.anchor_table_name  
AND colOrder.projection_name = proj.projection_name  
AND colOrder.table_column_name = col.column_name  
ORDER BY tab.GB_Table DESC
  , tab.anchor_table_name
  , proj.GB_Proj DESC
  , proj.projection_name
  , col.MB_Col DESC
; 


-- ###Performance
--  **vRunningQueries**
--  **vLongRunningQueries**
--  **vQueryPlanProfiles**


--=========================================================================
--Create vRunningQueries
--=========================================================================
DROP VIEW IF EXISTS vstb.vRunningQueries;

CREATE VIEW vstb.vRunningQueries
AS 
SELECT
     node_name
    ,user_name
    ,'SELECT CLOSE_SESSION(''' || session_id || ''');'  AS CloseSession
    ,statement_start
    ,(GETDATE() - statement_start)::INTERVAL  AS current_statement_duration  
    ,REGEXP_REPLACE(current_statement,'[\r\n\t]',' ') AS current_statement 
    ,session_id
    ,transaction_id
    ,statement_id
    ,client_hostname
    ,login_timestamp
    ,runtime_priority
    ,ssl_state
    ,authentication_method
    ,transaction_start
    ,GETDATE() AS Today
FROM v_monitor.sessions
ORDER BY statement_start DESC 
;



--=========================================================================
--Create vLongRunningQueries
--=========================================================================
DROP VIEW IF EXISTS vstb.vLongRunningQueries;

CREATE VIEW vstb.vLongRunningQueries
AS 
SELECT
     node_name
    ,user_name
    ,start_timestamp
    ,end_timestamp
    ,(COALESCE(end_timestamp,GETDATE()) - start_timestamp)::INTERVAL AS request_duration
    ,request_duration_ms
    ,success
    ,is_executing
    ,REGEXP_REPLACE(request,'[\r\n\t]',' ') AS request    
    ,'SELECT CLOSE_SESSION(''' || session_id || ''');'  AS CloseSession
    ,session_id    
    ,transaction_id
    ,statement_id
    ,request_type
    ,request_label
    ,search_path
    ,memory_acquired_mb
    ,error_count
    ,request_id
    ,GETDATE() AS Today    
FROM v_monitor.query_requests 
ORDER BY request_duration_ms DESC 
;



--=========================================================================
--Create vQueryPlanProfiles
--=========================================================================
DROP VIEW IF EXISTS vstb.vQueryPlanProfiles;

CREATE VIEW vstb.vQueryPlanProfiles
AS 
SELECT qp.query_start
    ,qp.query_duration_us
    ,(COALESCE(qr.end_timestamp, GETDATE()) - qr.start_timestamp)::INTERVAL AS request_duration
    ,qp.user_name
    ,qp.is_executing
    ,qp.query_type
    ,qp.session_id
    ,qp.transaction_id
    ,qp.statement_id
    ,qp.identifier
    ,qp.node_name
    ,REGEXP_REPLACE(query, '[\r\n\t\f]', ' ') AS query
    ,qpp.path_line
    ,qpp.path_id
    ,qpp.path_line_index
    ,qpp.running_time
    ,qpp.memory_allocated_bytes
    ,qpp.read_from_disk_bytes
    ,qpp.received_bytes
    ,qpp.sent_bytes
    ,qe.event_messages
FROM v_monitor.query_profiles AS qp
JOIN v_monitor.query_plan_profiles AS qpp ON qp.statement_id = qpp.statement_id
    AND qp.transaction_id = qpp.transaction_id
JOIN v_monitor.query_requests AS qr ON qp.statement_id = qr.statement_id
    AND qp.transaction_id = qr.transaction_id
    AND qp.session_id = qr.session_id
LEFT JOIN

( 
  SELECT transaction_id
        ,statement_id
        ,session_id
        ,MAPTOSTRING(raw_map) AS event_messages
    FROM 
       ( 
          SELECT transaction_id
                ,statement_id
                ,session_id
                ,MAPAGGREGATE(event_description
                             , REGEXP_REPLACE(
                                               mapToSTring(raw_map using parameters canonical_json=true)::VARCHAR(65000)
                                              ,'[\r\n\t\f]',' ') 
                             ) 
                 OVER (PARTITION by transaction_id, statement_id, session_id) AS raw_map      
            FROM 
                 (
                   SELECT event_description
                         ,transaction_id
                         ,statement_id
                         ,session_id
                         ,MAPAGGREGATE(node_name , event_details ) 
                          OVER (PARTITION BY event_description, transaction_id, statement_id, session_id) AS raw_map
                   FROM v_monitor.query_events
                 ) AS T1
       ) AS T2
) AS qe       
ON qp.statement_id = qe.statement_id
    AND qp.transaction_id = qe.transaction_id
    AND qp.session_id = qe.session_id
WHERE 1 = 1
ORDER BY qp.query_start DESC
    ,qp.transaction_id
    ,qpp.path_line
    ,qpp.statement_id
    ,qpp.path_id
    ,qpp.path_line_index;


--=========================================================================
--Create vMostCommonQueries
--=========================================================================
DROP VIEW IF EXISTS vstb.vMostCommonQueries;

CREATE VIEW vstb.vMostCommonQueries
AS 
SELECT
     REGEXP_REPLACE(request,'[\r\n\t]',' ')  AS request   
    ,COUNT(*)                                AS queries    
    ,COUNT(DISTINCT user_name)               AS users
    ,MIN(start_timestamp)                    AS first_run
    ,MAX(end_timestamp)                      AS last_run
    ,AVG((COALESCE(end_timestamp,GETDATE()) - start_timestamp))::INTERVAL AS avg_request_duration    
    ,MIN((COALESCE(end_timestamp,GETDATE()) - start_timestamp))::INTERVAL AS min_request_duration
    ,MAX((COALESCE(end_timestamp,GETDATE()) - start_timestamp))::INTERVAL AS max_request_duration
    ,(AVG(request_duration_ms)/1e3)::INTEGER AS avg_request_duration_seconds    
    ,(MIN(request_duration_ms)/1e3)::INTEGER AS min_request_duration_seconds 
    ,(MAX(request_duration_ms)/1e3)::INTEGER AS max_request_duration_seconds 
    ,(SUM(request_duration_ms)/1e3)::INTEGER AS total_seconds
    ,SUM(memory_acquired_mb)                 AS total_memory_acquired_mb
FROM v_monitor.query_requests 
WHERE   1=1
    AND COALESCE(success,TRUE)=TRUE
    AND request not ilike 'commit%'
    AND request not ilike 'rollback%'
    AND request not ilike 'truncate%'
    AND request not ilike 'drop%'  
    AND request not ilike 'grant%'     
GROUP BY 1
ORDER BY COUNT(*) DESC 
;









