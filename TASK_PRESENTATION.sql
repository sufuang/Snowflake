/* 3. Create the PRESENTATION schema in SUPPLYCHAIN DB
      FACTS/DIMS with star schema model
*/

--set the context
use role sysadmin;
use database supplychain;
use warehouse transform_wh;

create schema PRESENTATION;

--create the facts and dims

--1.item table
create or replace table item(
    VendorProductID text not null, --item dim
    InternalItemId text not null, --item dim
    BranchItemCd text not null, --item dim
    ItemDescription text not null --item dim
);
truncate table item;

--2. retail reference table
create or replace table retailref(
    divisionid text not null, --retail dim
    rogcd text not null, --retail dim
    upcid text not null, --retail dim
    retailsectioncd text not null, --retail dim
    retailsectionnm text not null --retail dim
);
truncate table retailref;

--create an error log table to catch errors from task
create or replace table error_log (error_code number, error_state string, error_message string, stack_trace string);


--create a stored proc to 
--1. detect new xml rows from FLATTENED schema
--3. merge/insert into facts/dims
--5. tag query statements in the warehouse
--6. log error into a table
create or replace procedure PRESENTATION_SP0_FACTS_DIMS()
    returns string
    language javascript
    strict
    execute as caller
    as
    $$
    var spname = arguments.callee.toString().match(/function\s(\w+)/)[1];
    
    var query_tag = 
     "alter session set query_tag='" + spname + "'";
     
    var query_untag = 
     "alter session unset query_tag";
    
    var sql_command = 
     "insert all " + 
        "into item(VendorProductID,InternalItemId,BranchItemCd,ItemDescription) values(VendorProductID,InternalItemId,BranchItemCd,ItemDescription) " + 
        " into retailref(divisionid,rogcd,upcid,retailsectioncd,retailsectionnm) values (divisionid,rogcd,upcid,retailsectioncd,retailsectionnm) " +  
        " select * from supplychain.transform.flattened_item_stream ";
    
    
    try {
        snowflake.execute (
            {sqlText: query_tag}
        );
        
 
        snowflake.execute (
            {sqlText: sql_command}
        );
            

        snowflake.execute (
            {sqlText: query_untag}
        );
        result = "Succeeded";
        }
    catch (err)  {
        snowflake.execute({
            sqlText: 'insert into error_log VALUES (?,?,?,?)',binds: [err.code, err.state, err.message, err.stackTraceTxt]
            });
            result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
            result += "\n  Message: " + err.message;
            result += "\nStack Trace:\n" + err.stackTraceTxt; 
         }
    
    return result;
    
    $$
    ;
    
--create a task on the stream
create or replace task PRESENTATION_T0_FACTS_DIMS
  WAREHOUSE = task_wh,
  SCHEDULE = '1 MINUTE'
  when system$stream_has_data('supplychain.transform.flattened_item_stream')
  AS
    call PRESENTATION_SP0_FACTS_DIMS();
    
alter task PRESENTATION_T0_FACTS_DIMS suspend;

-- When will the next task run?
select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state
from table(information_schema.task_history()) where state = 'SCHEDULED' and name = 'PRESENTATION_T0_FACTS_DIMS' order by completed_time desc;

select * from table(information_schema.task_history()) where name = 'PRESENTATION_T0_FACTS_DIMS';

select * from supplychain.presentation.item;

select * from supplychain.presentation.retailref;

--simple stored proc
create or replace procedure sp_pi()
    returns float not null
    language javascript
    as
    $$
    return 3.1415926;
    $$
    ;

--another simple task

create or replace table mytable( ts timestamp_ltz);

CREATE OR REPLACE TASK PRESENTATION_T0_mytask_minute
  WAREHOUSE = load_wh,
  SCHEDULE = '15 MINUTE'
AS
INSERT INTO mytable(ts) VALUES(CURRENT_TIMESTAMP);

--resume this before demo
alter task PRESENTATION_T0_mytask_minute suspend;


