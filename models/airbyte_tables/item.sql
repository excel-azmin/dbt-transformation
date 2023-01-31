
{{ config(materialized='table') }}


with __dbt__cte__item_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "postgres".public._airbyte_raw_item
select
    jsonb_extract_path_text(_airbyte_data, 'name') as "name",
    jsonb_extract_path(_airbyte_data, 'uoms') as uoms,
    jsonb_extract_path(_airbyte_data, 'uoms') ->> 'uoms_name' as uoms_name,
    jsonb_extract_path(_airbyte_data, 'uoms') ->> 'modified_by' as uoms_item_modified_by,
    jsonb_extract_path_text(_airbyte_data, 'uuid') as uuid,
    jsonb_extract_path_text(_airbyte_data, 'brand') as brand,
    jsonb_extract_path_text(_airbyte_data, 'image') as image,
    jsonb_extract_path_text(_airbyte_data, 'owner') as "owner",
    jsonb_extract_path(_airbyte_data, 'taxes') as taxes,
    jsonb_extract_path_text(_airbyte_data, 'doctype') as doctype,
    jsonb_extract_path(_airbyte_data, 'barcodes') as barcodes,
    jsonb_extract_path_text(_airbyte_data, 'creation') as creation,
    jsonb_extract_path_text(_airbyte_data, 'disabled') as disabled,
    jsonb_extract_path_text(_airbyte_data, 'isSynced') as issynced,
    jsonb_extract_path_text(_airbyte_data, 'modified') as modified,
    jsonb_extract_path_text(_airbyte_data, 'docstatus') as docstatus,
    jsonb_extract_path_text(_airbyte_data, 'item_code') as item_code,
    jsonb_extract_path_text(_airbyte_data, 'item_name') as item_name,
    jsonb_extract_path_text(_airbyte_data, 'stock_uom') as stock_uom,
    jsonb_extract_path_text(_airbyte_data, 'thumbnail') as thumbnail,
    jsonb_extract_path_text(_airbyte_data, 'weightage') as weightage,
    jsonb_extract_path(_airbyte_data, 'attributes') as "attributes",
    jsonb_extract_path_text(_airbyte_data, 'item_group') as item_group,
    jsonb_extract_path_text(_airbyte_data, 'description') as description,
    jsonb_extract_path_text(_airbyte_data, 'end_of_life') as end_of_life,
    jsonb_extract_path_text(_airbyte_data, 'modified_by') as modified_by,
    jsonb_extract_path(_airbyte_data, 'bundle_items') as bundle_items,
    jsonb_extract_path_text(_airbyte_data, 'has_batch_no') as has_batch_no,
    jsonb_extract_path_text(_airbyte_data, 'has_variants') as has_variants,
    jsonb_extract_path_text(_airbyte_data, 'max_discount') as max_discount,
    jsonb_extract_path_text(_airbyte_data, 'minimumPrice') as minimumprice,
    jsonb_extract_path_text(_airbyte_data, 'no_of_months') as no_of_months,
    jsonb_extract_path_text(_airbyte_data, 'safety_stock') as safety_stock,
    jsonb_extract_path_text(_airbyte_data, 'customer_code') as customer_code,
    jsonb_extract_path_text(_airbyte_data, 'has_serial_no') as has_serial_no,
    jsonb_extract_path_text(_airbyte_data, 'is_sales_item') as is_sales_item,
    jsonb_extract_path_text(_airbyte_data, 'is_stock_item') as is_stock_item,
    jsonb_extract_path(_airbyte_data, 'item_defaults') as item_defaults,
    jsonb_extract_path_text(_airbyte_data, 'min_order_qty') as min_order_qty,
    jsonb_extract_path_text(_airbyte_data, 'naming_series') as naming_series,
    jsonb_extract_path_text(_airbyte_data, 'opening_stock') as opening_stock,
    jsonb_extract_path_text(_airbyte_data, 'retain_sample') as retain_sample,
    jsonb_extract_path_text(_airbyte_data, 'website_image') as website_image,
    jsonb_extract_path_text(_airbyte_data, 'asset_category') as asset_category,
    jsonb_extract_path(_airbyte_data, 'customer_items') as customer_items,
    jsonb_extract_path_text(_airbyte_data, 'is_fixed_asset') as is_fixed_asset,
    jsonb_extract_path_text(_airbyte_data, 'lead_time_days') as lead_time_days,
    jsonb_extract_path_text(_airbyte_data, 'publish_in_hub') as publish_in_hub,
    jsonb_extract_path(_airbyte_data, 'reorder_levels') as reorder_levels,
    jsonb_extract_path(_airbyte_data, 'supplier_items') as supplier_items,
    jsonb_extract_path_text(_airbyte_data, 'has_expiry_date') as has_expiry_date,
    jsonb_extract_path_text(_airbyte_data, 'sample_quantity') as sample_quantity,
    jsonb_extract_path_text(_airbyte_data, 'show_in_website') as show_in_website,
    jsonb_extract_path_text(_airbyte_data, 'synced_with_hub') as synced_with_hub,
    jsonb_extract_path_text(_airbyte_data, 'warranty_period') as warranty_period,
    jsonb_extract_path_text(_airbyte_data, 'weight_per_unit') as weight_per_unit,
    jsonb_extract_path_text(_airbyte_data, 'create_new_batch') as create_new_batch,
    jsonb_extract_path_text(_airbyte_data, 'is_item_from_hub') as is_item_from_hub,
    jsonb_extract_path_text(_airbyte_data, 'is_purchase_item') as is_purchase_item,
    jsonb_extract_path_text(_airbyte_data, 'no_of_months_exp') as no_of_months_exp,
    jsonb_extract_path_text(_airbyte_data, 'valuation_method') as valuation_method,
    jsonb_extract_path_text(_airbyte_data, 'variant_based_on') as variant_based_on,
    jsonb_extract_path_text(_airbyte_data, 'country_of_origin') as country_of_origin,
    jsonb_extract_path_text(_airbyte_data, 'has_excel_serials') as has_excel_serials,
    jsonb_extract_path_text(_airbyte_data, 'auto_create_assets') as auto_create_assets,
    jsonb_extract_path_text(_airbyte_data, 'shelf_life_in_days') as shelf_life_in_days,
    jsonb_extract_path_text(_airbyte_data, 'asset_naming_series') as asset_naming_series,
    jsonb_extract_path_text(_airbyte_data, 'salesWarrantyMonths') as saleswarrantymonths,
    jsonb_extract_path_text(_airbyte_data, 'total_projected_qty') as total_projected_qty,
    jsonb_extract_path(_airbyte_data, 'website_item_groups') as website_item_groups,
    jsonb_extract_path_text(_airbyte_data, 'delivered_by_supplier') as delivered_by_supplier,
    jsonb_extract_path_text(_airbyte_data, 'allow_alternative_item') as allow_alternative_item,
    jsonb_extract_path_text(_airbyte_data, 'is_sub_contracted_item') as is_sub_contracted_item,
    jsonb_extract_path_text(_airbyte_data, 'over_billing_allowance') as over_billing_allowance,
    jsonb_extract_path_text(_airbyte_data, 'purchaseWarrantyMonths') as purchasewarrantymonths,
    jsonb_extract_path(_airbyte_data, 'website_specifications') as website_specifications,
    jsonb_extract_path_text(_airbyte_data, 'enable_deferred_expense') as enable_deferred_expense,
    jsonb_extract_path_text(_airbyte_data, 'enable_deferred_revenue') as enable_deferred_revenue,
    jsonb_extract_path_text(_airbyte_data, 'show_variant_in_website') as show_variant_in_website,
    jsonb_extract_path_text(_airbyte_data, 'is_customer_provided_item') as is_customer_provided_item,
    jsonb_extract_path_text(_airbyte_data, 'default_material_request_type') as default_material_request_type,
    jsonb_extract_path_text(_airbyte_data, 'include_item_in_manufacturing') as include_item_in_manufacturing,
    jsonb_extract_path_text(_airbyte_data, 'standard_rate_aibyte_transform') as standard_rate_aibyte_transform,
    jsonb_extract_path_text(_airbyte_data, 'over_delivery_receipt_allowance') as over_delivery_receipt_allowance,
    jsonb_extract_path_text(_airbyte_data, 'valuation_rate_aibyte_transform') as valuation_rate_aibyte_transform,
    jsonb_extract_path_text(_airbyte_data, 'inspection_required_before_delivery') as inspection_required_before_delivery,
    jsonb_extract_path_text(_airbyte_data, 'inspection_required_before_purchase') as inspection_required_before_purchase,
    jsonb_extract_path_text(_airbyte_data, 'last_purchase_rate_aibyte_transform') as last_purchase_rate_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "postgres".public._airbyte_raw_item as table_alias
-- item
where 1 = 1
),  __dbt__cte__item_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__item_ab1
select
    
    cast("name" as text) as "name",
    uoms,
    uoms_name,
    uoms_item_modified_by,
    cast(uuid as text) as uuid,
    cast(brand as text) as brand,
    cast(image as text) as image,
    cast("owner" as text) as "owner",
    taxes,
    cast(doctype as text) as doctype,
    barcodes,
    cast(creation as text) as creation,
    cast(disabled as 
    float
) as disabled,
    cast(issynced as boolean) as issynced,
    cast(modified as text) as modified,
    cast(docstatus as 
    float
) as docstatus,
    cast(item_code as text) as item_code,
    cast(item_name as text) as item_name,
    cast(stock_uom as text) as stock_uom,
    cast(thumbnail as text) as thumbnail,
    cast(weightage as 
    float
) as weightage,
    "attributes",
    cast(item_group as text) as item_group,
    cast(description as text) as description,
    cast(end_of_life as text) as end_of_life,
    cast(modified_by as text) as modified_by,
    bundle_items,
    cast(has_batch_no as 
    float
) as has_batch_no,
    cast(has_variants as 
    float
) as has_variants,
    cast(max_discount as 
    float
) as max_discount,
    cast(minimumprice as 
    float
) as minimumprice,
    cast(no_of_months as 
    float
) as no_of_months,
    cast(safety_stock as 
    float
) as safety_stock,
    cast(customer_code as text) as customer_code,
    cast(has_serial_no as 
    float
) as has_serial_no,
    cast(is_sales_item as 
    float
) as is_sales_item,
    cast(is_stock_item as 
    float
) as is_stock_item,
    item_defaults,
    cast(min_order_qty as 
    float
) as min_order_qty,
    cast(naming_series as text) as naming_series,
    cast(opening_stock as 
    float
) as opening_stock,
    cast(retain_sample as 
    float
) as retain_sample,
    cast(website_image as text) as website_image,
    cast(asset_category as text) as asset_category,
    customer_items,
    cast(is_fixed_asset as 
    float
) as is_fixed_asset,
    cast(lead_time_days as 
    float
) as lead_time_days,
    cast(publish_in_hub as 
    float
) as publish_in_hub,
    reorder_levels,
    supplier_items,
    cast(has_expiry_date as 
    float
) as has_expiry_date,
    cast(sample_quantity as 
    float
) as sample_quantity,
    cast(show_in_website as 
    float
) as show_in_website,
    cast(synced_with_hub as 
    float
) as synced_with_hub,
    cast(warranty_period as text) as warranty_period,
    cast(weight_per_unit as 
    float
) as weight_per_unit,
    cast(create_new_batch as 
    float
) as create_new_batch,
    cast(is_item_from_hub as 
    float
) as is_item_from_hub,
    cast(is_purchase_item as 
    float
) as is_purchase_item,
    cast(no_of_months_exp as 
    float
) as no_of_months_exp,
    cast(valuation_method as text) as valuation_method,
    cast(variant_based_on as text) as variant_based_on,
    cast(country_of_origin as text) as country_of_origin,
    cast(has_excel_serials as text) as has_excel_serials,
    cast(auto_create_assets as 
    float
) as auto_create_assets,
    cast(shelf_life_in_days as 
    float
) as shelf_life_in_days,
    cast(asset_naming_series as text) as asset_naming_series,
    cast(saleswarrantymonths as 
    float
) as saleswarrantymonths,
    cast(total_projected_qty as 
    float
) as total_projected_qty,
    website_item_groups,
    cast(delivered_by_supplier as 
    float
) as delivered_by_supplier,
    cast(allow_alternative_item as 
    float
) as allow_alternative_item,
    cast(is_sub_contracted_item as 
    float
) as is_sub_contracted_item,
    cast(over_billing_allowance as 
    float
) as over_billing_allowance,
    cast(purchasewarrantymonths as 
    float
) as purchasewarrantymonths,
    website_specifications,
    cast(enable_deferred_expense as 
    float
) as enable_deferred_expense,
    cast(enable_deferred_revenue as 
    float
) as enable_deferred_revenue,
    cast(show_variant_in_website as 
    float
) as show_variant_in_website,
    cast(is_customer_provided_item as 
    float
) as is_customer_provided_item,
    cast(default_material_request_type as text) as default_material_request_type,
    cast(include_item_in_manufacturing as 
    float
) as include_item_in_manufacturing,
    cast(standard_rate_aibyte_transform as text) as standard_rate_aibyte_transform,
    cast(over_delivery_receipt_allowance as 
    float
) as over_delivery_receipt_allowance,
    cast(valuation_rate_aibyte_transform as text) as valuation_rate_aibyte_transform,
    cast(inspection_required_before_delivery as 
    float
) as inspection_required_before_delivery,
    cast(inspection_required_before_purchase as 
    float
) as inspection_required_before_purchase,
    cast(last_purchase_rate_aibyte_transform as text) as last_purchase_rate_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__item_ab1
-- item
where 1 = 1
),  __dbt__cte__item_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__item_ab2
select
    md5(cast(coalesce(cast("name" as text), '') || '-' || coalesce(cast(uoms as text), '') || '-' || coalesce(cast(uuid as text), '') || '-' || coalesce(cast(brand as text), '') || '-' || coalesce(cast(image as text), '') || '-' || coalesce(cast("owner" as text), '') || '-' || coalesce(cast(taxes as text), '') || '-' || coalesce(cast(doctype as text), '') || '-' || coalesce(cast(barcodes as text), '') || '-' || coalesce(cast(creation as text), '') || '-' || coalesce(cast(disabled as text), '') || '-' || coalesce(cast(issynced as text), '') || '-' || coalesce(cast(modified as text), '') || '-' || coalesce(cast(docstatus as text), '') || '-' || coalesce(cast(item_code as text), '') || '-' || coalesce(cast(item_name as text), '') || '-' || coalesce(cast(stock_uom as text), '') || '-' || coalesce(cast(thumbnail as text), '') || '-' || coalesce(cast(weightage as text), '') || '-' || coalesce(cast("attributes" as text), '') || '-' || coalesce(cast(item_group as text), '') || '-' || coalesce(cast(description as text), '') || '-' || coalesce(cast(end_of_life as text), '') || '-' || coalesce(cast(modified_by as text), '') || '-' || coalesce(cast(bundle_items as text), '') || '-' || coalesce(cast(has_batch_no as text), '') || '-' || coalesce(cast(has_variants as text), '') || '-' || coalesce(cast(max_discount as text), '') || '-' || coalesce(cast(minimumprice as text), '') || '-' || coalesce(cast(no_of_months as text), '') || '-' || coalesce(cast(safety_stock as text), '') || '-' || coalesce(cast(customer_code as text), '') || '-' || coalesce(cast(has_serial_no as text), '') || '-' || coalesce(cast(is_sales_item as text), '') || '-' || coalesce(cast(is_stock_item as text), '') || '-' || coalesce(cast(item_defaults as text), '') || '-' || coalesce(cast(min_order_qty as text), '') || '-' || coalesce(cast(naming_series as text), '') || '-' || coalesce(cast(opening_stock as text), '') || '-' || coalesce(cast(retain_sample as text), '') || '-' || coalesce(cast(website_image as text), '') || '-' || coalesce(cast(asset_category as text), '') || '-' || coalesce(cast(customer_items as text), '') || '-' || coalesce(cast(is_fixed_asset as text), '') || '-' || coalesce(cast(lead_time_days as text), '') || '-' || coalesce(cast(publish_in_hub as text), '') || '-' || coalesce(cast(reorder_levels as text), '') || '-' || coalesce(cast(supplier_items as text), '') || '-' || coalesce(cast(has_expiry_date as text), '') || '-' || coalesce(cast(sample_quantity as text), '') || '-' || coalesce(cast(show_in_website as text), '') || '-' || coalesce(cast(synced_with_hub as text), '') || '-' || coalesce(cast(warranty_period as text), '') || '-' || coalesce(cast(weight_per_unit as text), '') || '-' || coalesce(cast(create_new_batch as text), '') || '-' || coalesce(cast(is_item_from_hub as text), '') || '-' || coalesce(cast(is_purchase_item as text), '') || '-' || coalesce(cast(no_of_months_exp as text), '') || '-' || coalesce(cast(valuation_method as text), '') || '-' || coalesce(cast(variant_based_on as text), '') || '-' || coalesce(cast(country_of_origin as text), '') || '-' || coalesce(cast(has_excel_serials as text), '') || '-' || coalesce(cast(auto_create_assets as text), '') || '-' || coalesce(cast(shelf_life_in_days as text), '') || '-' || coalesce(cast(asset_naming_series as text), '') || '-' || coalesce(cast(saleswarrantymonths as text), '') || '-' || coalesce(cast(total_projected_qty as text), '') || '-' || coalesce(cast(website_item_groups as text), '') || '-' || coalesce(cast(delivered_by_supplier as text), '') || '-' || coalesce(cast(allow_alternative_item as text), '') || '-' || coalesce(cast(is_sub_contracted_item as text), '') || '-' || coalesce(cast(over_billing_allowance as text), '') || '-' || coalesce(cast(purchasewarrantymonths as text), '') || '-' || coalesce(cast(website_specifications as text), '') || '-' || coalesce(cast(enable_deferred_expense as text), '') || '-' || coalesce(cast(enable_deferred_revenue as text), '') || '-' || coalesce(cast(show_variant_in_website as text), '') || '-' || coalesce(cast(is_customer_provided_item as text), '') || '-' || coalesce(cast(default_material_request_type as text), '') || '-' || coalesce(cast(include_item_in_manufacturing as text), '') || '-' || coalesce(cast(standard_rate_aibyte_transform as text), '') || '-' || coalesce(cast(over_delivery_receipt_allowance as text), '') || '-' || coalesce(cast(valuation_rate_aibyte_transform as text), '') || '-' || coalesce(cast(inspection_required_before_delivery as text), '') || '-' || coalesce(cast(inspection_required_before_purchase as text), '') || '-' || coalesce(cast(last_purchase_rate_aibyte_transform as text), '') as text)) as _airbyte_item_hashid,
    tmp.*
from __dbt__cte__item_ab2 tmp
-- item
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__item_ab3
select
    "name",
    jsonb_array_elements(uoms) ->> 'uom' uoms,
    jsonb_array_elements(uoms) ->> 'name' uoms_name,
    jsonb_array_elements(uoms) ->> 'modified_by' uoms_item_modified_by,
    uuid,
    brand,
    image,
    "owner",
    taxes,
    doctype,
    barcodes,
    creation,
    disabled,
    issynced,
    modified,
    docstatus,
    item_code,
    item_name,
    stock_uom,
    thumbnail,
    weightage,
    "attributes",
    item_group,
    description,
    end_of_life,
    modified_by,
    bundle_items,
    has_batch_no,
    has_variants,
    max_discount,
    minimumprice,
    no_of_months,
    safety_stock,
    customer_code,
    has_serial_no,
    is_sales_item,
    is_stock_item,
    item_defaults,
    min_order_qty,
    naming_series,
    opening_stock,
    retain_sample,
    website_image,
    asset_category,
    customer_items,
    is_fixed_asset,
    lead_time_days,
    publish_in_hub,
    reorder_levels,
    supplier_items,
    has_expiry_date,
    sample_quantity,
    show_in_website,
    synced_with_hub,
    warranty_period,
    weight_per_unit,
    create_new_batch,
    is_item_from_hub,
    is_purchase_item,
    no_of_months_exp,
    valuation_method,
    variant_based_on,
    country_of_origin,
    has_excel_serials,
    auto_create_assets,
    shelf_life_in_days,
    asset_naming_series,
    saleswarrantymonths,
    total_projected_qty,
    website_item_groups,
    delivered_by_supplier,
    allow_alternative_item,
    is_sub_contracted_item,
    over_billing_allowance,
    purchasewarrantymonths,
    website_specifications,
    enable_deferred_expense,
    enable_deferred_revenue,
    show_variant_in_website,
    is_customer_provided_item,
    default_material_request_type,
    include_item_in_manufacturing,
    standard_rate_aibyte_transform,
    over_delivery_receipt_allowance,
    valuation_rate_aibyte_transform,
    inspection_required_before_delivery,
    inspection_required_before_purchase,
    last_purchase_rate_aibyte_transform,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_item_hashid
from __dbt__cte__item_ab3
-- item from "postgres".public._airbyte_raw_item
where 1 = 1
