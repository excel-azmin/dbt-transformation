
{{ config(materialized='table') }}

with __dbt__cte__serial_no_ab1 as (


select
    jsonb_extract_path_text(_airbyte_data, 'serial_no') as serial_no,
    jsonb_extract_path_text(_airbyte_data, 'item_code') as item_code,
    jsonb_extract_path_text(_airbyte_data, 'item_name') as item_name
from "postgres".public._airbyte_raw_serial_no as table_alias
-- item
where 1 = 1
),  __dbt__cte__serial_no_ab2 as (        

select
    serial_no,
    item_code,
    item_name
from __dbt__cte__serial_no_ab1
-- item
where 1 = 1
),  __dbt__cte__serial_no_ab3 as (    

select
    md5(cast(coalesce(cast("serial_no" as text), '') || '-' 
    || coalesce(cast(item_code as text), '') || '-' 
    || coalesce(cast(item_name as text), '') as text)) as _airbyte_serial_no_hashid,
    tmp.*
from __dbt__cte__serial_no_ab2 tmp
-- item
where 1 = 1
)    

select
    serial_no,
    item_code,
    item_name
from __dbt__cte__serial_no_ab3
-- item from "postgres".public._airbyte_raw_item
where 1 = 1
