--==============================================================================
-- Diagnostic Views - Including Historical data
--
-- **vstb.vProjectionSizeHistory**
-- **vstb.vProjectionUsageHistory** 
-- **vstb.vLongRunningQueriesHistory**
-- **vstb.vProjectionGrowthOverTimeWeekly**
--==============================================================================
 

--=========================================================================
--Create vProjectionSizeHistory
--=========================================================================
DROP VIEW IF EXISTS vstb.vProjectionSizeHistory ;

CREATE VIEW vstb.vProjectionSizeHistory AS
SELECT
    anchor_table_schema || '.' || anchor_table_name AS anchor_table_schema_name
   ,projection_schema || '.' || projection_name AS projection_schema_name
   ,last_refresh_ts
   ,SUM(row_count)               AS row_count
   ,SUM(used_bytes)/(1e9::FLOAT) AS used_Gb
   ,SUM(ros_count)               AS ros_count_sum
   ,MIN(ros_count)               AS ros_count_min
   ,MIN(ros_count)               AS ros_count_max
FROM  vstb.projection_storage
GROUP BY 1,2,3
ORDER BY 1,2,3;

--=========================================================================
--Create vProjectionUsageHistory
--=========================================================================
DROP VIEW IF EXISTS vstb.vProjectionUsageHistory ;

CREATE VIEW vstb.vProjectionUsageHistory 
AS
SELECT 
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
         FROM v_monitor.projection_usage
       UNION
         SELECT
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
         FROM vstb.projection_usage ) AS B1
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
         FROM v_monitor.projection_usage 
       UNION 
         SELECT
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
         FROM vstb.projection_usage ) AS B1
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



-- ###Performance
--  **vRunningQueries**
--  **vLongRunningQueries**
--  **vQueryPlanProfiles**
--  **vLongRunningQueriesHistory**
--  **vQueryPlanProfilesHistory**

--=========================================================================
--Create vLongRunningQueriesHistory
--=========================================================================
DROP VIEW IF EXISTS vstb.vLongRunningQueriesHistory;

CREATE VIEW vstb.vLongRunningQueriesHistory
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
FROM query_requests 
UNION 
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
FROM vstb.query_requests   
ORDER BY request_duration_ms DESC 
;


       
--=========================================================================
--Create vProjectionGrowthOverTimeWeekly
--This table measures compressed storage over multiple projections
--Space usage reported is not RAW data.
--=========================================================================
DROP VIEW IF EXISTS vstb.vProjectionGrowthOverTimeWeekly;

CREATE VIEW vstb.vProjectionGrowthOverTimeWeekly
AS
SELECT 
       node_name
      ,projection_name
      ,projection_schema
      ,anchor_table_name
      ,anchor_table_schema
      ,slice_time_week      
      ,used_bytes
      ,used_bytes/1E9 AS used_GB
      ,LAG(used_bytes,1,0) OVER (
       PARTITION BY 
            node_name
           ,projection_name
           ,projection_schema
           ,anchor_table_name
           ,anchor_table_schema 
        ORDER BY slice_time_week ASC) AS used_bytes_previous
      ,used_bytes -  
      LAG(used_bytes,1,0) OVER (
       PARTITION BY 
            node_name
           ,projection_name
           ,projection_schema
           ,anchor_table_name
           ,anchor_table_schema 
        ORDER BY slice_time_week ASC) AS difference
FROM (
SELECT slice_time::DATE+6 as slice_time_week
      ,node_name
      ,projection_name
      ,projection_schema
      ,anchor_table_name
      ,anchor_table_schema
      ,TS_LAST_VALUE(used_bytes, 'CONST') AS used_bytes
      ,TS_LAST_VALUE(row_count,  'CONST') AS row_count
FROM vstb.projection_storage
WHERE 1 = 1
  AND projection_schema NOT LIKE 'v_%' 
TIMESERIES slice_time AS '1 WEEK' 
    OVER (PARTITION BY 
            node_name
           ,projection_name
           ,projection_schema
           ,anchor_table_name
           ,anchor_table_schema 
         ORDER BY last_refresh_ts) 
    ) AS T 
ORDER BY   
       node_name
      ,projection_name
      ,projection_schema
      ,anchor_table_name
      ,anchor_table_schema
      ,slice_time_week   
;











