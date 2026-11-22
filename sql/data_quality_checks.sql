-- Executable Snowflake data-quality demonstration using synthetic rows.
-- Replace sample_data with a real staging table to adapt for production.
with sample_data as (
    select column1::integer as order_id,
           column2::varchar as customer_id,
           column3::number(10, 2) as amount
    from values
        (1, 'A', 25.50),
        (2, 'B', 11.00),
        (2, 'B', 11.00),
        (3, null, -5.00)
)
select
    count(*) as total_rows,
    count_if(order_id is null) as null_order_ids,
    count_if(customer_id is null) as null_customers,
    count_if(amount < 0) as negative_amounts,
    count(*) - count(distinct order_id) as duplicate_order_ids
from sample_data;
