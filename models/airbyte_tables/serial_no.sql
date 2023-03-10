
{{ config(materialized='table') }}

with __dbt__cte__serial_no_ab1 as (


select
    jsonb_extract_path_text(_airbyte_data, 'serial_no') as serial_no,
    jsonb_extract_path_text(_airbyte_data, 'item_code') as item_code,
    jsonb_extract_path_text(_airbyte_data, 'item_name') as item_name,
    jsonb_extract_path(_airbyte_data, 'warranty')  warranty
from "postgres".public._airbyte_raw_serial_no as table_alias
-- item
where 1 = 1
)  

select
    serial_no,
    item_code,
    item_name,
    warranty ->> 'purchaseWarrantyDate' as warranty_date,
    warranty ->> 'purchasedOn' as purchasedOn
from __dbt__cte__serial_no_ab1
-- item from "postgres".public._airbyte_raw_item
where 1 = 1
