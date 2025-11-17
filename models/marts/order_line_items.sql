with orders as (
    select * from {{ ref('stg_orders') }}
),

lineitems as (
    select * from {{ ref('stg_lineitem') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
)

select
    lineitems.order_key,
    lineitems.line_number,
    orders.customer_key,
    orders.order_status,
    orders.order_date,
    orders.order_priority,
    orders.clerk,
    orders.ship_priority,
    lineitems.part_key,
    lineitems.supplier_key,
    lineitems.quantity,
    lineitems.extended_price,
    lineitems.discount,
    lineitems.tax,
    lineitems.extended_price * (1 - lineitems.discount) as discounted_price,
    lineitems.extended_price * (1 - lineitems.discount) * (1 + lineitems.tax) as final_price,
    lineitems.return_flag,
    lineitems.line_status,
    lineitems.ship_date,
    lineitems.commit_date,
    lineitems.receipt_date,
    lineitems.ship_instruct,
    lineitems.ship_mode,
    customers.nation_name,
    customers.region_name,
    customers.market_segment
from lineitems
left join orders on lineitems.order_key = orders.order_key
left join customers on orders.customer_key = customers.customer_key


