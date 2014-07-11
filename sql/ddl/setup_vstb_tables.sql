--=============================================================================
-- Objective: Monitor Query Activity, System Performance, and Space Usage
--            over time
--=============================================================================
--=============================================================================
-- Create the v_monitor_history schema
--=============================================================================
CREATE SCHEMA vstb;

--=============================================================================
-- Create the v_monitor table structures
--   execution_engine_profiles
--   query_profiles
--   query_plan_profiles
--   query_requests
--   query_events
--   resource_rejection_details
--   user_sessions
--   transactions
--   load_streams
--   projection_storage
--   projection_usage
--   system_resource_usage
--   resource_acquisitions
-- Create the data collector table structures
--   dc_execution_engine_events
--=============================================================================
DROP TABLE IF EXISTS vstb.execution_engine_profiles CASCADE;

CREATE TABLE vstb.execution_engine_profiles (
         node_name VARCHAR(128) ENCODING RLE
        ,user_id INT ENCODING RLE
        ,user_name VARCHAR(128) ENCODING RLE
        ,session_id VARCHAR(128) ENCODING RLE
        ,transaction_id INT ENCODING DELTARANGE_COMP
        ,statement_id INT ENCODING RLE
        ,plan_id INT ENCODING BLOCKDICT_COMP
        ,operator_name VARCHAR(128) ENCODING RLE
        ,operator_id INT ENCODING COMMONDELTA_COMP
        ,baseplan_id INT ENCODING COMMONDELTA_COMP
        ,path_id INT ENCODING COMMONDELTA_COMP
        ,localplan_id INT ENCODING COMMONDELTA_COMP
        ,activity_id INT ENCODING DELTARANGE_COMP
        ,resource_id INT ENCODING RLE
        ,counter_name VARCHAR(128) ENCODING AUTO
        ,counter_tag VARCHAR(128) ENCODING AUTO
        ,counter_value INT ENCODING DELTARANGE_COMP
        ,is_executing boolean ENCODING RLE
)
    ORDER BY transaction_id
        ,node_name
        ,user_id
        ,user_name
        ,session_id
        ,statement_id
        ,plan_id
        ,operator_name
        ,operator_id
        ,baseplan_id
        ,path_id
        ,localplan_id
        ,activity_id
        ,resource_id
        ,counter_name
        ,counter_tag
        ,counter_value
        ,is_executing 
SEGMENTED BY HASH (transaction_id) ALL NODES;

DROP TABLE IF EXISTS vstb.query_profiles CASCADE;
 
CREATE TABLE vstb.query_profiles (
     session_id VARCHAR(128) ENCODING AUTO
    ,transaction_id INT ENCODING COMMONDELTA_COMP
    ,statement_id INT ENCODING COMMONDELTA_COMP
    ,identifier VARCHAR(128) ENCODING RLE
    ,node_name VARCHAR(128) ENCODING RLE
    ,query VARCHAR(64000) ENCODING AUTO
    ,query_search_path VARCHAR(64000) ENCODING RLE
    ,schema_name VARCHAR(128) ENCODING RLE
    ,table_name VARCHAR(128) ENCODING AUTO
    ,projections_used VARCHAR(22) ENCODING RLE
    ,query_duration_us NUMERIC(36) ENCODING AUTO
    ,query_start_epoch INT ENCODING COMMONDELTA_COMP
    ,query_start VARCHAR(63) ENCODING AUTO
    ,query_type VARCHAR(128) ENCODING RLE
    ,error_code INT ENCODING BLOCKDICT_COMP
    ,user_name VARCHAR(128) ENCODING RLE
    ,processed_row_count INT ENCODING DELTARANGE_COMP
    ,reserved_extra_memory INT ENCODING BLOCKDICT_COMP
    ,is_executing boolean ENCODING RLE
    )
ORDER BY transaction_id
    ,session_id
    ,statement_id
    ,identifier
    ,node_name
    ,query
    ,query_search_path
    ,schema_name
    ,table_name
    ,projections_used
    ,query_duration_us
    ,query_start_epoch
    ,query_start
    ,query_type
    ,error_code
    ,user_name
    ,processed_row_count
    ,reserved_extra_memory
    ,is_executing 
SEGMENTED BY HASH (transaction_id) ALL NODES;

DROP TABLE IF EXISTS vstb.query_plan_profiles CASCADE;

CREATE TABLE vstb.query_plan_profiles (
     transaction_id INT ENCODING DELTARANGE_COMP
    ,statement_id INT ENCODING RLE
    ,path_id INT ENCODING COMMONDELTA_COMP
    ,path_line_index INT ENCODING RLE
    ,path_is_started boolean ENCODING RLE
    ,path_is_completed boolean ENCODING RLE
    ,is_executing boolean ENCODING RLE
    ,running_time interval ENCODING DELTARANGE_COMP
    ,memory_allocated_bytes INT ENCODING DELTARANGE_COMP
    ,read_from_disk_bytes INT ENCODING RLE
    ,received_bytes INT ENCODING RLE
    ,sent_bytes INT ENCODING RLE
    ,path_line VARCHAR(64000) ENCODING AUTO
    )
ORDER BY transaction_id
    ,statement_id
    ,path_id
    ,path_line_index
    ,path_is_started
    ,path_is_completed
    ,is_executing
    ,running_time
    ,memory_allocated_bytes
    ,read_from_disk_bytes
    ,received_bytes
    ,sent_bytes
    ,path_line
SEGMENTED BY HASH (transaction_id) ALL NODES;
 

DROP TABLE IF EXISTS vstb.query_requests CASCADE;

CREATE TABLE vstb.query_requests (
     node_name VARCHAR(128) ENCODING RLE
    ,user_name VARCHAR(128) ENCODING RLE
    ,session_id VARCHAR(128) ENCODING AUTO
    ,request_id INT ENCODING COMMONDELTA_COMP
    ,transaction_id INT ENCODING COMMONDELTA_COMP
    ,statement_id INT ENCODING COMMONDELTA_COMP
    ,request_type VARCHAR(128) ENCODING RLE
    ,request VARCHAR(64000) ENCODING AUTO
    ,request_label VARCHAR(128) ENCODING RLE
    ,search_path VARCHAR(64000) ENCODING RLE
    ,memory_acquired_mb FLOAT ENCODING COMMONDELTA_COMP
    ,success boolean ENCODING RLE
    ,error_count INT ENCODING RLE
    ,start_timestamp timestamptz ENCODING DELTARANGE_COMP
    ,end_timestamp timestamptz ENCODING DELTARANGE_COMP
    ,request_duration_ms INT ENCODING DELTARANGE_COMP
    ,is_executing boolean ENCODING RLE
)
ORDER BY transaction_id
    ,node_name
    ,user_name
    ,session_id
    ,request_id
    ,statement_id
    ,request_type
    ,request
    ,request_label
    ,search_path
    ,memory_acquired_mb
    ,success
    ,error_count
    ,start_timestamp
    ,end_timestamp
    ,request_duration_ms
    ,is_executing 
SEGMENTED BY HASH (transaction_id) ALL NODES;

DROP TABLE IF EXISTS vstb.query_events CASCADE;

CREATE TABLE vstb.query_events (
     event_timestamp timestamptz ENCODING DELTARANGE_COMP
    ,node_name VARCHAR(128) ENCODING RLE
    ,user_id INT ENCODING RLE
    ,user_name VARCHAR(128) ENCODING RLE
    ,session_id VARCHAR(128) ENCODING AUTO
    ,request_id INT ENCODING COMMONDELTA_COMP
    ,transaction_id INT ENCODING DELTARANGE_COMP
    ,statement_id INT ENCODING RLE
    ,event_category VARCHAR(12) ENCODING RLE
    ,event_type VARCHAR(64000) ENCODING AUTO
    ,event_description VARCHAR(64000) ENCODING AUTO
    ,operator_name VARCHAR(128) ENCODING AUTO
    ,path_id INT ENCODING COMMONDELTA_COMP
    ,object_id INT ENCODING DELTARANGE_COMP
    ,event_details VARCHAR(64000) ENCODING AUTO
    ,suggested_action VARCHAR(64000) ENCODING RLE
    )
ORDER BY transaction_id
    ,event_timestamp
    ,node_name
    ,user_id
    ,user_name
    ,session_id
    ,request_id
    ,statement_id
    ,event_category
    ,event_type
    ,event_description
    ,operator_name
    ,path_id
    ,object_id
    ,event_details
    ,suggested_action
SEGMENTED BY HASH (transaction_id) ALL NODES;

DROP TABLE IF EXISTS vstb.resource_rejection_details CASCADE;

CREATE TABLE vstb.resource_rejection_details (
    rejected_timestamp timestamptz ENCODING AUTO
    ,node_name VARCHAR(128) ENCODING RLE
    ,user_name VARCHAR(128) ENCODING RLE
    ,session_id VARCHAR(128) ENCODING RLE
    ,request_id INT ENCODING AUTO
    ,transaction_id INT ENCODING AUTO
    ,statement_id INT ENCODING AUTO
    ,pool_id INT ENCODING RLE
    ,pool_name VARCHAR(128) ENCODING RLE
    ,reason VARCHAR(128) ENCODING RLE
    ,resource_type VARCHAR(128) ENCODING RLE
    ,rejected_value INT ENCODING AUTO
)  
ORDER BY transaction_id
    ,rejected_timestamp
    ,node_name
    ,user_name
    ,session_id
    ,request_id
    ,statement_id
    ,pool_id
    ,pool_name
    ,reason
    ,resource_type
    ,rejected_value 
SEGMENTED BY HASH (transaction_id) ALL NODES;

DROP TABLE IF EXISTS vstb.user_sessions CASCADE;

CREATE TABLE vstb.user_sessions (
    node_name VARCHAR(128) ENCODING RLE
    ,user_name VARCHAR(128) ENCODING RLE
    ,session_id VARCHAR(128) ENCODING AUTO
    ,transaction_id INT ENCODING AUTO
    ,statement_id INT ENCODING AUTO
    ,session_start_timestamp timestamptz ENCODING AUTO
    ,session_end_timestamp timestamptz ENCODING AUTO
    ,is_active boolean ENCODING RLE
    ,client_hostname VARCHAR(128) ENCODING AUTO
    ,client_pid INT ENCODING RLE
    ,client_label VARCHAR(64000) ENCODING AUTO
    ,ssl_state VARCHAR(128) ENCODING RLE
    ,authentication_method VARCHAR(128) ENCODING RLE
 ) 
ORDER BY session_id
    ,user_name
    ,node_name 
SEGMENTED BY HASH (session_id) ALL NODES;

DROP TABLE IF EXISTS vstb.transactions CASCADE;

CREATE TABLE vstb.transactions (
     start_timestamp    timestamptz ENCODING    DELTARANGE_COMP 
    ,end_timestamp  timestamptz ENCODING    DELTARANGE_COMP 
    ,node_name  VARCHAR(128)    ENCODING    RLE 
    ,user_id    INT ENCODING    RLE 
    ,user_name  VARCHAR(128)    ENCODING    RLE 
    ,session_id VARCHAR(128)    ENCODING    AUTO    
    ,transaction_id INT ENCODING    COMMONDELTA_COMP    
    ,description    VARCHAR(64000)  ENCODING    AUTO    
    ,start_epoch    INT ENCODING    COMMONDELTA_COMP    
    ,end_epoch  INT ENCODING    COMMONDELTA_COMP    
    ,number_of_statements   INT ENCODING    RLE 
    ,ISOLATION  VARCHAR(128)    ENCODING    RLE 
    ,is_read_only   boolean ENCODING    RLE 
    ,is_committed   boolean ENCODING    RLE 
    ,is_local   boolean ENCODING    RLE 
    ,is_initiator   boolean ENCODING    RLE 
    ,is_ddl boolean ENCODING    RLE 
) 
ORDER BY transaction_id
    ,start_timestamp
    ,end_timestamp
    ,node_name
    ,user_id
    ,user_name
    ,session_id
    ,description
    ,start_epoch
    ,end_epoch
    ,number_of_statements
    ,ISOLATION
    ,is_read_only
    ,is_committed
    ,is_local
    ,is_initiator
    ,is_ddl 
SEGMENTED BY HASH (transaction_id) ALL NODES;

DROP TABLE IF EXISTS vstb.load_streams CASCADE;

CREATE TABLE vstb.load_streams (
    session_id VARCHAR(128) ENCODING RLE
    ,transaction_id INT ENCODING COMMONDELTA_COMP
    ,statement_id INT ENCODING RLE
    ,stream_name VARCHAR(128) ENCODING RLE
    ,schema_name VARCHAR(128) ENCODING RLE
    ,table_id INT ENCODING COMMONDELTA_COMP
    ,table_name VARCHAR(128) ENCODING AUTO
    ,load_start VARCHAR(63) ENCODING AUTO
    ,load_duration_ms NUMERIC(54) ENCODING RLE
    ,is_executing boolean ENCODING RLE
    ,accepted_row_count INT ENCODING COMMONDELTA_COMP
    ,rejected_row_count INT ENCODING RLE
    ,read_bytes INT ENCODING AUTO
    ,input_file_size_bytes INT ENCODING COMMONDELTA_COMP
    ,parse_complete_percent INT ENCODING RLE
    ,unsorted_row_count INT ENCODING COMMONDELTA_COMP
    ,sorted_row_count INT ENCODING COMMONDELTA_COMP
    ,sort_complete_percent INT ENCODING RLE
)
ORDER BY transaction_id
    ,session_id
    ,statement_id
    ,stream_name
    ,schema_name
    ,table_id
    ,table_name
    ,load_start
    ,load_duration_ms
    ,is_executing
    ,accepted_row_count
    ,rejected_row_count
    ,read_bytes
    ,input_file_size_bytes
    ,parse_complete_percent
    ,unsorted_row_count
    ,sorted_row_count
    ,sort_complete_percent 
SEGMENTED BY HASH (transaction_id) ALL NODES;


DROP TABLE IF EXISTS vstb.projection_storage CASCADE;

CREATE TABLE vstb.projection_storage (
    node_name VARCHAR(128) ENCODING RLE
    ,projection_id INT ENCODING AUTO
    ,projection_name VARCHAR(128) ENCODING AUTO
    ,projection_schema VARCHAR(128) ENCODING AUTO
    ,projection_column_count INT ENCODING AUTO
    ,row_count INT ENCODING AUTO
    ,used_bytes INT ENCODING AUTO
    ,wos_row_count INT ENCODING RLE
    ,wos_used_bytes INT ENCODING RLE
    ,ros_row_count INT ENCODING AUTO
    ,ros_used_bytes INT ENCODING AUTO
    ,ros_count INT ENCODING COMMONDELTA_COMP
    ,anchor_table_name VARCHAR(128) ENCODING AUTO
    ,anchor_table_schema VARCHAR(128) ENCODING AUTO
    ,anchor_table_id INT ENCODING AUTO
    ,last_refresh_ts DATETIME ENCODING RLE
)
ORDER BY projection_id
    ,node_name
    ,last_refresh_ts
    ,anchor_table_name
    ,projection_name
    ,projection_schema
    ,anchor_table_schema
    ,projection_column_count
    ,anchor_table_id 
SEGMENTED BY HASH (
         projection_id
        ,node_name
        ,last_refresh_ts
) ALL NODES;

DROP TABLE IF EXISTS vstb.projection_usage CASCADE;

CREATE TABLE vstb.projection_usage (
    query_start_timestamp timestamptz ENCODING DELTARANGE_COMP
    ,node_name VARCHAR(128) ENCODING RLE
    ,user_name VARCHAR(128) ENCODING RLE
    ,session_id VARCHAR(128) ENCODING AUTO
    ,request_id INT ENCODING COMMONDELTA_COMP
    ,transaction_id INT ENCODING COMMONDELTA_COMP
    ,statement_id INT ENCODING COMMONDELTA_COMP
    ,io_type VARCHAR(128) ENCODING RLE
    ,projection_id INT ENCODING COMMONDELTA_COMP
    ,projection_name VARCHAR(128) ENCODING AUTO
    ,anchor_table_id INT ENCODING COMMONDELTA_COMP
    ,anchor_table_schema VARCHAR(128) ENCODING RLE
    ,anchor_table_name VARCHAR(128) ENCODING AUTO
)
ORDER BY anchor_table_schema
    ,anchor_table_name
    ,projection_name
    ,node_name
    ,user_name
    ,query_start_timestamp
    ,io_type
    ,session_id
    ,request_id
    ,transaction_id
    ,statement_id
    ,projection_id
    ,anchor_table_id 
SEGMENTED BY HASH (anchor_table_name) ALL NODES;

DROP TABLE IF EXISTS vstb.dc_execution_engine_events CASCADE;

CREATE TABLE vstb.dc_execution_engine_events (
    "time" timestamptz ENCODING DELTARANGE_COMP
    ,node_name VARCHAR(128) ENCODING RLE
    ,session_id VARCHAR(128) ENCODING AUTO
    ,user_id INT ENCODING RLE
    ,user_name VARCHAR(128) ENCODING RLE
    ,transaction_id INT ENCODING DELTARANGE_COMP
    ,statement_id INT ENCODING RLE
    ,request_id INT ENCODING COMMONDELTA_COMP
    ,event_type VARCHAR(128) ENCODING AUTO
    ,event_description VARCHAR(512) ENCODING AUTO
    ,operator_name VARCHAR(128) ENCODING RLE
    ,path_id INT ENCODING COMMONDELTA_COMP
    ,event_oid INT ENCODING DELTARANGE_COMP
    ,event_details VARCHAR(1024) ENCODING AUTO
    ,suggested_action VARCHAR(1024) ENCODING RLE
)
ORDER BY transaction_id
    ,session_id
    ,statement_id
    ,request_id
    ,"time"
    ,node_name
    ,user_id
    ,user_name
    ,event_type
    ,event_description
    ,operator_name
    ,path_id
    ,event_oid
    ,event_details
    ,suggested_action 
SEGMENTED BY HASH (transaction_id) ALL NODES;


DROP TABLE IF EXISTS vstb.system_resource_usage CASCADE;

CREATE TABLE vstb.system_resource_usage
(
    node_name varchar(128) ENCODING RLE, 
    end_time timestamp ENCODING COMMONDELTA_COMP,
    average_memory_usage_percent float ENCODING COMMONDELTA_COMP,
    average_cpu_usage_percent float,
    net_rx_kbytes_per_second float,
    net_tx_kbytes_per_second float,
    io_read_kbytes_per_second float,
    io_written_kbytes_per_second float
)
ORDER BY node_name, end_time
SEGMENTED BY HASH(node_name, end_time) ALL NODES
;



DROP TABLE IF EXISTS vstb.resource_acquisitions CASCADE;

CREATE TABLE vstb.resource_acquisitions
(
    node_name varchar(128) ENCODING RLE,
    transaction_id int ENCODING COMMONDELTA_COMP,
    statement_id int ENCODING COMMONDELTA_COMP,
    request_type varchar(128),
    pool_id int ENCODING COMMONDELTA_COMP,
    pool_name varchar(128),
    thread_count int ENCODING COMMONDELTA_COMP,
    open_file_handle_count int ENCODING COMMONDELTA_COMP,
    memory_inuse_kb int ENCODING COMMONDELTA_COMP,
    queue_entry_timestamp timestamptz ENCODING DELTARANGE_COMP,
    acquisition_timestamp timestamptz ENCODING DELTARANGE_COMP,
    release_timestamp timestamptz ENCODING DELTARANGE_COMP,
    duration_ms int ENCODING DELTARANGE_COMP,
    is_executing boolean ENCODING BLOCKDICT_COMP
) ORDER BY transaction_id, statement_id, queue_entry_timestamp, is_executing, node_name, 
           request_type, pool_name, acquisition_timestamp, release_timestamp
SEGMENTED BY HASH(transaction_id, statement_id, queue_entry_timestamp) ALL NODES ;



--SELECT MARK_DESIGN_KSAFE(0);


-- SELECT MARK_DESIGN_KSAFE(1);
--=============================================================================
-- Turn on Profiling
--=============================================================================
SELECT SHOW_PROFILING_CONFIG();

SELECT SET_CONFIG_PARAMETER('GlobalSessionProfiling', 1);
SELECT SET_CONFIG_PARAMETER('GlobalQueryProfiling', 1);
SELECT SET_CONFIG_PARAMETER('GlobalEEProfiling', 1);


    /****************************************************************************************************
--=============================================================================
-- Initial Setup
--=============================================================================
;drop table vstb.dc_execution_engine_events
;drop table vstb.execution_engine_profiles
;drop table vstb.load_streams
;drop table vstb.projection_storage
;drop table vstb.projection_usage
;drop table vstb.query_events
;drop table vstb.query_plan_profiles
;drop table vstb.query_profiles
;drop table vstb.query_requests
;drop table vstb.resource_rejection_details
;drop table vstb.transactions
;drop table vstb.user_sessions 
;
 
 
 
select * into vstb.dc_execution_engine_events from dc_execution_engine_events           WHERE 1=0;
select * into vstb.dc_projections_used        from dc_projections_used                  WHERE 1=0;
select * into vstb.load_streams               from v_monitor.load_streams               WHERE 1=0;
select * into vstb.execution_engine_profiles  from v_monitor.execution_engine_profiles  WHERE 1=0;
select * into vstb.query_profiles             from v_monitor.query_profiles             WHERE 1=0;
select * into vstb.query_plan_profiles        from v_monitor.query_plan_profiles        WHERE 1=0;
select * into vstb.query_requests             from v_monitor.query_requests             WHERE 1=0;
select * into vstb.query_events               from v_monitor.query_events               WHERE 1=0;
select * into vstb.resource_rejection_details from v_monitor.resource_rejection_details WHERE 1=0;
select * into vstb.sessions                   from v_monitor.sessions                   WHERE 1=0;
select * into vstb.user_sessions              from v_monitor.user_sessions              WHERE 1=0;
select * into vstb.transactions               from v_monitor.transactions               WHERE 1=0;
select *, GETDATE()::DATE AS last_refresh_ts into vstb.projection_storage from v_monitor.projection_storage  ;

--******************************************************************************/
