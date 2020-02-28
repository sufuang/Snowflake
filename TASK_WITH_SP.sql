/* 3. Create the TRANSFORM schema in SUPPLYCHAIN DB
      This will serve as an integration/tranformation layer
      Flatten the variant XML, could merge datasets
      Record errors during flatten
*/

--set the context
use role sysadmin;
use database supplychain;
use warehouse transform_wh;
use schema transform;

--create flattened table
create or replace TABLE ITEMMASTER_FLATTENED (
	CREATIONDT DATE not null,
	DISTRIBUTIONCENTERID VARCHAR(16777216) not null,
	WAREHOUSEID NUMBER(38,0) not null,
	VENDORID VARCHAR(16777216) not null,
	PRODUCTIONWEIGHT FLOAT not null,
	INTERNETITEMDSC VARCHAR(16777216) not null,
    VendorProductID text not null, --item dim
    InternalItemId text not null, --item dim
    BranchItemCd text not null, --item dim
    ItemDescription text not null, --item dim
    divisionid text not null, --retail dim
    rogcd text not null, --retail dim
    upcid text not null, --retail dim
    retailsectioncd text not null, --retail dim
    retailsectionnm text not null --retail dim
  
);
truncate table itemmaster_flattened;

create or replace stream flattened_item_stream on table itemmaster_flattened;

--create an error log table to catch errors from task
create or replace table error_log (error_code number, error_state string, error_message string, stack_trace string);


--create a stored proc to 
--1. detect new xml rows from CONFORM schema
--2. scale the warehouse up
--3. flatten xml into structured table
--4. scale warehouse down
--5. tag query statements in the warehouse
--6. log error into a table
create or replace procedure TRANSFORM_SP0_FLATTEN_XML()
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
     
    var scaleup_sql = 
     "alter warehouse task_wh set warehouse_size=large";
     
    var scaledown_sql = 
     "alter warehouse task_wh set warehouse_size=small";
    
    var sql_command = 
     "insert into itemmaster_flattened(CreationDt,DistributionCenterId,WarehouseId,VendorId,ProductionWeight,InternetItemDsc,VendorProductID,InternalItemId,BranchItemCd,ItemDescription,DivisionId,ROGCd,UPCID,RetailSectionCd,RetailSectionNm) " + 
        "select CreationDt,DistributionCenterId,WarehouseId,VendorId,ProductionWeight,InternetItemDsc,VendorProductID,InternalItemId,BranchItemCd,ItemDescription,DivisionId,ROGCd,UPCID,RetailSectionCd,RetailSectionNm from ( " +
          "WITH " + 
          "documentdata(docData) as " +
              "(select XMLGET(V,'DocumentData') from supplychain.conform.valid_item_stream where METADATA$ACTION='INSERT'), " +
          "supplychain(itemData) as " +
              "(select XMLGET(V,'SupplyChainItemData') from supplychain.conform.valid_item_stream where METADATA$ACTION='INSERT'), " +
          "flatsupplychain(val,k) as " +
              "(select value:\"$\" as val, value:\"@\" as k from supplychain,LATERAL FLATTEN( itemData:\"$\",recursive=>true ) child " +
               "union " +
               "select value:\"$\" as val, value:\"@\" as k from documentdata, LATERAL FLATTEN( docData:\"$\",recursive=>true ) child2 ) " +
          "select CreationDt::date as CreationDt," +
                  "DistributionCenterId::string as DistributionCenterId,WarehouseId::integer as WarehouseId," +
                  "VendorId::string as VendorId,ProductionWeight::float as ProductionWeight,InternetItemDsc::string as InternetItemDsc," +
                  "VendorProductID::string as VendorProductID ,InternalItemId::string as InternalItemId, BranchItemCd::string as BranchItemCd, ItemDescription::string as  ItemDescription," + 
                  "DivisionId::string as DivisionId,ROGCd::string as ROGCd,UPCID::string as UPCID,RetailSectionCd::string as RetailSectionCd,RetailSectionNm::string as RetailSectionNm " + 
            "from flatsupplychain " +
                  "pivot(max(val) for k in ('Abs:CreationDt','Abs:DistributionCenterId','Abs:WarehouseId','Abs:VendorId','Abs:ProductionWeight','Abs:InternetItemDsc','Abs:VendorProductID', 'Abs:InternalItemId', 'Abs:BranchItemCd','Abs:ItemDescription','Abs:DivisionId','Abs:ROGCd','Abs:UPCID','Abs:RetailSectionCd','Abs:RetailSectionNm')) " +
                  "as p(CreationDt,DistributionCenterId,WarehouseId,VendorId,ProductionWeight,InternetItemDsc,VendorProductID,InternalItemId,BranchItemCd,ItemDescription,DivisionId,ROGCd,UPCID,RetailSectionCd,RetailSectionNm )" +
          ")";
   
    try {
        snowflake.execute (
            {sqlText: query_tag}
        );
        
        snowflake.execute (
            {sqlText: scaleup_sql}
        );
 
        snowflake.execute (
            {sqlText: sql_command}
        );
            
        snowflake.execute (
            {sqlText: scaledown_sql}
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
create or replace task TRANSFORM_T0_FLATTEN_XML
  WAREHOUSE = task_wh,
  SCHEDULE = '1 MINUTE'
  when system$stream_has_data('supplychain.conform.valid_item_stream')
  AS
    call TRANSFORM_SP0_FLATTEN_XML();
    
alter task TRANSFORM_T0_FLATTEN_XML suspend;

-- When will the next task run?
select timestampdiff(second, current_timestamp, scheduled_time) as next_run, scheduled_time, current_timestamp, name, state
from table(information_schema.task_history()) where state = 'SCHEDULED' and name = 'TRANSFORM_T0_FLATTEN_XML' order by completed_time desc;

select * from table(information_schema.task_history()) where name = 'TRANSFORM_T0_FLATTEN_XML';

select * from supplychain.transform.itemmaster_flattened;

select * from supplychain.transform.flattened_item_stream;


select * from table(information_schema.task_history());

--test--

 select CreationDt,DistributionCenterId,WarehouseId,VendorId,ProductionWeight,InternetItemDsc,VendorProductID,InternalItemId,BranchItemCd,
    ItemDescription,DivisionId,ROGCd,UPCID,RetailSectionCd,RetailSectionNm 
    from 
    ( WITH documentdata(docData) as (select XMLGET(V,'DocumentData') from supplychain.conform.valid_item), 
            supplychain(itemData) as (select XMLGET(V,'SupplyChainItemData') from supplychain.conform.valid_item), 
            flatsupplychain(val,k) as (select value:"$" as val, value:"@" as k from supplychain,LATERAL FLATTEN( itemData:"$",recursive=>true ) child union 
                                       select value:"$" as val, value:"@" as k from documentdata, LATERAL FLATTEN( docData:"$",recursive=>true ) child2 ) 
     select CreationDt::date as CreationDt,DistributionCenterId::string as DistributionCenterId,WarehouseId::integer as WarehouseId,VendorId::string as VendorId,
            ProductionWeight::float as ProductionWeight,InternetItemDsc::string as InternetItemDsc,VendorProductID::string as VendorProductID ,
            InternalItemId::string as InternalItemId, BranchItemCd::string as BranchItemCd, ItemDescription::string as  ItemDescription,
            DivisionId::string as DivisionId,ROGCd::string as ROGCd,UPCID::string as UPCID,RetailSectionCd::string as RetailSectionCd,
            RetailSectionNm::string as RetailSectionNm 
     from flatsupplychain pivot(max(val) for k in ('Abs:CreationDt','Abs:DistributionCenterId','Abs:WarehouseId','Abs:VendorId','Abs:ProductionWeight','Abs:InternetItemDsc','Abs:VendorProductID', 'Abs:InternalItemId', 'Abs:BranchItemCd','Abs:ItemDescription','Abs:DivisionId','Abs:ROGCd','Abs:UPCID','Abs:RetailSectionCd','Abs:RetailSectionNm')) as p(CreationDt,DistributionCenterId,WarehouseId,VendorId,ProductionWeight,InternetItemDsc,VendorProductID,InternalItemId,BranchItemCd,ItemDescription,DivisionId,ROGCd,UPCID,RetailSectionCd,RetailSectionNm ));