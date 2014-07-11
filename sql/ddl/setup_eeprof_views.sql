-- Copyright (c) 2011 Vertica Systems, Inc. Billerica, Masschusetts USA

-- Description: demo_eeprof_view.sql

-- This example script is governed by the same terms as other utilities
-- in /opt/vertica/scripts. See ‘Additional Terms for Vertica Support
-- Utilities’ in the Vertica Software License.

-- Create Date: 5/3/11

-- The views included in this script assist with querying the execution_engine_profiles
-- table. The views are created in the vstb schema which is not in the default search path.
-- 
-- There is one view for each profiling counter -- for example eeprof_execution_time_us.  
-- Multiple profiling counter views can be easily joined together with "natural left outer join" 
-- where the leftmost view is a view that would include all operators such as eeprof_operators.   
--
-- The eeprof_execution_time_us_rank view provides an easy way to see the operators that
-- have the highest execution times on each node.
-- 
-- These views are not supported and may change in future releases.
--


select add_vertica_options('BASIC', 'CREATE_SYSTEM_SCHEMA');
CREATE SCHEMA IF NOT EXISTS vstb;
select clr_vertica_options('BASIC', 'CREATE_SYSTEM_SCHEMA');

-- one view per counter, phj counters are excluded

CREATE OR REPLACE VIEW vstb.v_eeprof_bytes_received as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as bytes_received, is_executing   
from execution_engine_profiles where counter_name = 'bytes received';

CREATE OR REPLACE VIEW vstb.v_eeprof_bytes_sent as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as bytes_sent, is_executing   
from execution_engine_profiles where counter_name = 'bytes sent';

CREATE OR REPLACE VIEW vstb.v_eeprof_bytes_total as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as bytes_total, is_executing    
from execution_engine_profiles where counter_name = 'bytes total';

CREATE OR REPLACE VIEW vstb.v_eeprof_clock_time_us as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as clock_time_us, is_executing    
from execution_engine_profiles where counter_name = 'clock time (us)';

CREATE OR REPLACE VIEW vstb.v_eeprof_completed_merge_phases as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as completed_merge_phases, is_executing  
from execution_engine_profiles where counter_name = 'completed merge phases';

CREATE OR REPLACE VIEW vstb.v_eeprof_cumulative_size_of_raw_temp_data_bytes as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as cumulative_size_of_raw_temp_data_bytes, is_executing   
from execution_engine_profiles where counter_name = 'cumulative size of raw temp data (bytes)';

CREATE OR REPLACE VIEW vstb.v_eeprof_cumulative_size_of_temp_files_bytes as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as cumulative_size_of_temp_files_bytes, is_executing   
from execution_engine_profiles where counter_name = 'cumulative size of temp files (bytes)';

CREATE OR REPLACE VIEW vstb.v_eeprof_current_size_of_temp_files_bytes as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as current_size_of_temp_files_bytes, is_executing    
from execution_engine_profiles where counter_name = 'current size of temp files (bytes)';

CREATE OR REPLACE VIEW vstb.v_eeprof_execution_time_us as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as execution_time_us, is_executing    
from execution_engine_profiles where counter_name = 'execution time (us)';

CREATE OR REPLACE VIEW vstb.v_eeprof_files_completed as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as files_completed, is_executing    
from execution_engine_profiles where counter_name = 'files completed';

CREATE OR REPLACE VIEW vstb.v_eeprof_files_total as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as files_total, is_executing    
from execution_engine_profiles where counter_name = 'files total';

CREATE OR REPLACE VIEW vstb.v_eeprof_input_queue_wait_us as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as input_queue_wait_us, is_executing    
from execution_engine_profiles where counter_name = 'input queue wait (us)';

CREATE OR REPLACE VIEW vstb.v_eeprof_output_queue_wait_us as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as output_queue_wait_us, is_executing    
from execution_engine_profiles where counter_name = 'output queue wait (us)';

CREATE OR REPLACE VIEW vstb.v_eeprof_read_bytes as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as read_bytes, is_executing    
from execution_engine_profiles where counter_name = 'read_bytes';

CREATE OR REPLACE VIEW vstb.v_eeprof_receive_time_us as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as receive_time_us, is_executing    
from execution_engine_profiles where counter_name = 'receive time (us)';

CREATE OR REPLACE VIEW vstb.v_eeprof_rows_produced as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as rows_produced, is_executing    
from execution_engine_profiles where counter_name = 'rows produced';

CREATE OR REPLACE VIEW vstb.v_eeprof_estimated_rows_produced as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as est_rows_produced, is_executing    
from execution_engine_profiles where counter_name = 'estimated rows produced';

CREATE OR REPLACE VIEW vstb.v_eeprof_rows_rejected as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as rows_rejected, is_executing    
from execution_engine_profiles where counter_name = 'rows rejected';

CREATE OR REPLACE VIEW vstb.v_eeprof_send_time_us as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as send_time_us, is_executing    
from execution_engine_profiles where counter_name = 'send time (us)';

CREATE OR REPLACE VIEW vstb.v_eeprof_total_merge_phases as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as total_merge_phases, is_executing    
from execution_engine_profiles where counter_name = 'total merge phases';

CREATE OR REPLACE VIEW vstb.v_eeprof_WOS_bytes_acquired as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as WOS_bytes_acquired, is_executing 
from execution_engine_profiles where counter_name = 'WOS bytes acquired';

CREATE OR REPLACE VIEW vstb.v_eeprof_WOS_bytes_written as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as WOS_bytes_written, is_executing  
from execution_engine_profiles where counter_name = 'WOS bytes written';

CREATE OR REPLACE VIEW vstb.v_eeprof_memory_allocated_bytes as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as memory_allocated_bytes, is_executing  
from execution_engine_profiles where counter_name = 'memory allocated (bytes)';

CREATE OR REPLACE VIEW vstb.v_eeprof_memory_reserved_bytes as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as memory_reserved_bytes, is_executing  
from execution_engine_profiles where counter_name = 'memory reserved (bytes)';

CREATE OR REPLACE VIEW vstb.v_eeprof_file_handles as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as file_handles, is_executing  
from execution_engine_profiles where counter_name = 'file handles';

CREATE OR REPLACE VIEW vstb.v_eeprof_written_rows as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as written_rows, is_executing  
from execution_engine_profiles where counter_name = 'written rows';

CREATE OR REPLACE VIEW vstb.v_eeprof_chunk_longest_scan as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as chunk_longest_scan, is_executing  
from execution_engine_profiles where counter_name = 'chunk longest scan';

CREATE OR REPLACE VIEW vstb.v_eeprof_chunk_rows_scanned_squared as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as chunk_rows_scanned_squared, is_executing  
from execution_engine_profiles where counter_name = 'chunk rows scanned squared';

CREATE OR REPLACE VIEW vstb.v_eeprof_chunk_scans_run as 
select node_name, session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, 
counter_value as chunk_scans_run, is_executing  
from execution_engine_profiles where counter_name = 'chunk scans run';



-- view to get distinct operators

CREATE OR REPLACE VIEW vstb.v_eeprof_operators as 
select distinct node_name,
session_id, transaction_id, statement_id, operator_name, operator_id, path_id, baseplan_id, is_executing from execution_engine_profiles;

-- view to combine all counter views into a single view

CREATE OR REPLACE VIEW vstb.v_eeprof_counters as 
select to_hex(v_eeprof_operators.transaction_id) transaction_id_hex, * from 
 vstb.v_eeprof_operators natural left outer join 
 vstb.v_eeprof_bytes_received natural left outer join 
 vstb.v_eeprof_bytes_sent natural left outer join 
 vstb.v_eeprof_bytes_total natural left outer join 
 vstb.v_eeprof_clock_time_us natural left outer join 
 vstb.v_eeprof_completed_merge_phases natural left outer join 
 vstb.v_eeprof_cumulative_size_of_raw_temp_data_bytes  natural left outer join 
 vstb.v_eeprof_cumulative_size_of_temp_files_bytes  natural left outer join 
 vstb.v_eeprof_current_size_of_temp_files_bytes  natural left outer join 
 vstb.v_eeprof_execution_time_us  natural left outer join 
 vstb.v_eeprof_files_completed natural left outer join 
 vstb.v_eeprof_files_total natural left outer join 
 vstb.v_eeprof_input_queue_wait_us natural left outer join 
 vstb.v_eeprof_output_queue_wait_us natural left outer join 
 vstb.v_eeprof_read_bytes natural left outer join 
 vstb.v_eeprof_receive_time_us natural left outer join 
 vstb.v_eeprof_rows_produced natural left outer join 
 vstb.v_eeprof_estimated_rows_produced natural left outer join 
 vstb.v_eeprof_rows_rejected natural left outer join 
 vstb.v_eeprof_send_time_us natural left outer join 
 vstb.v_eeprof_total_merge_phases natural left outer join
 vstb.v_eeprof_WOS_bytes_acquired natural left outer join 
 vstb.v_eeprof_WOS_bytes_written natural left outer join 
 vstb.v_eeprof_memory_allocated_bytes natural left outer join 
 vstb.v_eeprof_memory_reserved_bytes natural left outer join 
 vstb.v_eeprof_file_handles natural left outer join 
 vstb.v_eeprof_written_rows natural left outer join 
 vstb.v_eeprof_chunk_longest_scan natural left outer join 
 vstb.v_eeprof_chunk_rows_scanned_squared natural left outer join 
 vstb.v_eeprof_chunk_scans_run;


-- other helper views

-- operators sorted by execution time, grouped by node, and including row counts,
-- would typically use with "ORDER BY node_name, rk" and a predicate for a specific
-- transaction_id and statement_id

CREATE OR REPLACE VIEW vstb.v_eeprof_execution_time_us_rank as 
select rank() over (partition by transaction_id, statement_id, node_name 
order by execution_time_us desc) rk, transaction_id, statement_id, node_name, operator_name, operator_id, path_id, baseplan_id, execution_time_us, rows_produced, is_executing
from (select * from vstb.v_eeprof_execution_time_us natural left outer join vstb.v_eeprof_rows_produced) q;

-- operators sorted by clock time, grouped by node, and including row counts,
-- would typically use with "ORDER BY node_name, rk" and a predicate for a specific
-- transaction_id and statement_id

CREATE OR REPLACE VIEW vstb.v_eeprof_clock_time_us_rank as 
select rank() over (partition by transaction_id, statement_id, node_name 
order by clock_time_us desc) rk, transaction_id, statement_id, node_name, operator_name, operator_id, path_id, baseplan_id, clock_time_us, rows_produced, is_executing
from (select * from vstb.v_eeprof_clock_time_us natural left outer join vstb.v_eeprof_rows_produced) q;


--- what parts of each query execution consumed time (aka pivot)
CREATE OR REPLACE VIEW vstb.v_query_execution_breakdown
AS
SELECT 
       plan_step.time              as start_time,
       plan_step.node_name         as node_name,
       plan_step.transaction_id    as transaction_id, 
       plan_step.statement_id      as stmt_id, 
       plan_step.request_id        as req_id, 
       request.duration            as overall_duration,
       plan_step.duration          as plan_duration,
       populatevproj_step.duration as popvproj_duration,
       serialize_step.duration     as serialize_duration,
       prepareplan_step.duration   as prepareplan_duration,
       --pp_tablelocks.duration      as pp_tablelocks_duration,
       --pp_distplanner.duration     as pp_distplanner_duration,
       --pp_localplan.duration       as pp_localplan_duration,
       --pp_eecompile.duration       as pp_eecompile_duration,
       compileplan_step.duration   as compileplan_duration,
       executeplan_step.duration   as executeplan_duration,
       (request.duration - 
        nvl(plan_step.duration,          '0'::interval) - 
	nvl(populatevproj_step.duration, '0'::interval) -
	nvl(serialize_step.duration,     '0'::interval)     -
	nvl(prepareplan_step.duration,   '0'::interval) -  
	nvl(compileplan_step.duration,   '0'::interval) - 
	nvl(executeplan_step.duration,   '0'::interval)) as unaccounted_for_duration,
       request.request             as request,
       request.request_type        as request_type,
       errors.message              as error_message
       
FROM
  (  SELECT 
      node_name,  transaction_id, statement_id, request_id, 
      time, 
      completion_time - time as duration 
     FROM dc_query_executions 
     WHERE execution_step = 'Plan'
  ) as plan_step 
  LEFT JOIN
  (  SELECT 
      node_name,  transaction_id, statement_id, request_id, 
      completion_time - time as duration 
     FROM dc_query_executions 
     WHERE execution_step = 'PopulateVirtualProjection'
  ) as populatevproj_step 
  USING ( node_name,  transaction_id, statement_id, request_id)
  LEFT JOIN
  (  SELECT 
      node_name,  transaction_id, statement_id, request_id, 
      completion_time - time as duration 
     FROM dc_query_executions 
     WHERE execution_step = 'SerializePlan'
  ) as serialize_step 
  USING ( node_name,  transaction_id, statement_id, request_id)
  LEFT JOIN
  (  SELECT 
      node_name,  transaction_id, statement_id, request_id, 
      completion_time - time as duration 
     FROM dc_query_executions 
     WHERE execution_step = 'PreparePlan'
  ) as prepareplan_step 
  USING ( node_name,  transaction_id, statement_id, request_id)
  LEFT JOIN
  (  SELECT 
      node_name,  transaction_id, statement_id, request_id, 
      completion_time - time as duration 
     FROM dc_query_executions 
     WHERE execution_step = 'CompilePlan'
  ) as compileplan_step 
  USING ( node_name,  transaction_id, statement_id, request_id)
  LEFT JOIN
  (  SELECT 
      node_name,  transaction_id, statement_id, request_id, 
      completion_time - time as duration 
     FROM dc_query_executions 
     WHERE execution_step = 'ExecutePlan'
  ) as executeplan_step 
  USING ( node_name,  transaction_id, statement_id, request_id)
  LEFT JOIN
  (  SELECT 
      node_name,  transaction_id, statement_id, request_id, 
      completion_time - time as duration 
     FROM dc_query_executions 
     WHERE execution_step = 'PreparePlan:TakeTableLocks'
  ) as pp_tablelocks 
  USING ( node_name,  transaction_id, statement_id, request_id)
  LEFT JOIN
  (  SELECT 
      node_name,  transaction_id, statement_id, request_id, 
      completion_time - time as duration 
     FROM dc_query_executions 
     WHERE execution_step = 'PreparePlan:DistPlanner'
  ) as pp_distplanner
  USING ( node_name,  transaction_id, statement_id, request_id)
  LEFT JOIN
  (  SELECT 
      node_name,  transaction_id, statement_id, request_id, 
      completion_time - time as duration 
     FROM dc_query_executions 
     WHERE execution_step = 'PreparePlan:LocalPlan'
  ) as pp_localplan
  USING ( node_name,  transaction_id, statement_id, request_id)
  LEFT JOIN
  (  SELECT 
      node_name,  transaction_id, statement_id, request_id, 
      completion_time - time as duration 
     FROM dc_query_executions 
     WHERE execution_step = 'PreparePlan:EEcompile'
  ) as pp_eecompile
  USING ( node_name,  transaction_id, statement_id, request_id)
  JOIN -- subquery to get part of the query that was requested. Actual query text is truncated.
  (  SELECT 
      dri.node_name,  dri.transaction_id, dri.statement_id, dri.request_id, 
      drc.time - dri.time as duration,
      dri.request_type,
      replace(substr(dri.request,1,50), E'\n', ' ') as request
     FROM dc_requests_issued dri 
          JOIN dc_requests_completed drc 
          USING (node_name,  session_id, request_id)
  ) as request 
  USING ( node_name,  transaction_id, statement_id, request_id)
  LEFT JOIN 
  (  SELECT node_name,  transaction_id, statement_id, request_id,
            max(message) as message
     FROM dc_errors
     GROUP BY 1,2,3,4 -- ensure only a single error message
  ) as errors
  USING ( node_name,  transaction_id, statement_id, request_id)
ORDER BY plan_step.time 
;
