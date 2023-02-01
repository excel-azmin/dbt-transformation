
{{ config(materialized='table') }}

with __dbt__cte__sales_invoice_ab1 as (


select
    jsonb_extract_path(_airbyte_data, 'items') as items
from "postgres".public._airbyte_raw_sales_invoice as table_alias
-- item
where 1 = 1
)  

select
    jsonb_array_elements(items)->> 'name' as name,
    jsonb_array_elements(items)->> 'item_code' as item_code,
    jsonb_array_elements(items)->> 'item_name' as item_name,
    jsonb_array_elements(items)->> 'qty' as qty,
    jsonb_array_elements(items)->> 'amount' as amount,
    jsonb_array_elements(items)->> 'rate' as rate,
    jsonb_array_elements(items)->> 'has_serial_no' as has_serial_no,
    jsonb_array_elements(items)->> 'owner' as owner
from __dbt__cte__sales_invoice_ab1
-- item from "postgres".public._airbyte_raw_item
where 1 = 1
