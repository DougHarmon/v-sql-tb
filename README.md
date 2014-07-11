v-sql-tb
==========

Vertica SQL Toolbelt
--------------------
Why is this needed? DBA's need to be able to:

* Identify commonly used queries
* Identify opportunities for SQL tuning
* Identify resource utilization issues
* Identify table/projection usage
* Track table/projection disk space usage growth over time.

Also, Developers and Data Architects may not have pseudosuperuser rights
on the production database, but will want to know how their applications
perform in production so that they can tune them.


How is this done?
-----------------
There are several monitoring tools that work with Linux/Vertica such as:

* Management Console
* Ganglia
* Cacti

This tool is designed to complement these monitoring tools. It creates a set of
SQL tables to capture historical data and a SQL Load script to populate these 
tables. These enable retention of historical data on v\_monitor and data 
collector tables. A collection of diagnostic views that sit on top of 
the historical and the current v\_monitor and data collector tables provides 
the DBA with insights into the Vertica database.



Diagnostic Views
----------------

###Current - Not sourced from history tables

- **vProjectionUsage** 
- **vProjectionColumnSize** 
- **vRunningQueries**
- **vLongRunningQueries**
- **vQueryPlanProfiles**
- **vMostCommonQueries**
    
###Historical - Combine current data with historical

- **vProjectionSizeHistory**
- **vProjectionUsageHistory** 
- **vLongRunningQueriesHistory**
- **vProjectionGrowthOverTimeWeekly**



Tables And Views Created in vstb schema
---------------------------------------

###Tables sourced from v_monitor schema

- **execution\_engine\_profiles**
- **query\_profiles**
- **query\_plan\_profiles**
- **query\_requests**
- **query\_events**
- **resource\_rejection\_details**
- **user\_sessions**
- **transactions**
- **load\_streams**
- **projection\_storage**
- **projection\_usage**
- **system\_resource\_usage**
- **resource\_acquisitions**

###Tables sourced from Data Collector
 
- **dc\_execution\_engine\_events**


Directory Structure
-------------------

- **/bin** - bash shell scripts, typically called through cron jobs
- **/bin/ahm_lag_alert.sh** - raises an alert when ahm is 4 hours behind
- **/bin/vmart_vertica2vertica_parallel_load.sh** - parallel loader for copying tables to Vertica from Vertica
- **/sql** - sql scripts
- **/sql/ddl** - one time setup (DDL)
- **/sql/merge** - SQL scripts
- **/sql/merge/load_vstb_tables.sql** -  Loads historical tables in vstb schema
- **/sql/merge/update_statistics.sql** - Updates stats on tables missing stats
- **/sql/merge/purge_deleted_records.sql** - Purges deleted records from tables
- **/log** - log files are stored here. You will need to create this.



## FAQ

###Why not modify the history retention parameter in the data collector tables?
An alternative to creating custom history tables is to find the data collection
table that make up the v_monitor objects and extend the history retention
period for those tables. 

You can see how much history is retained in each data collector table by either
querying the time field in each dc table or looking at this view:
```
SELECT * FROM v_monitor.data_collector ORDER BY 3,1;
```

You can use EXPLAIN to find the data collector tables used by each v_monitor table 
```
EXPLAIN SELECT * FROM v_monitor.query_plan_profiles;
```
This package gives us the capability to save disk space by only capturing
history for specific events. 

###Why do you strip out carriage returns, line feeds and tabs from the query string?
Stripping out these characters makes it possible to copy and paste the results 
into Excel using Excel's default parsing options. If we left those characters
in then columns would be misaligned. You can reformat the SQL using an 
automated formatting tool such as the Poor Man's T-SQL Formatter 
plugin for notepad++.


