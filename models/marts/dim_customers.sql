with customers as (
    select * from {{ ref('stg_customer') }}
),

nations as (
    select * from {{ ref('stg_nation') }}
),

regions as (
    select * from {{ ref('stg_region') }}
)

select
    customers.customer_key,
    customers.customer_name,
    customers.customer_address,
    customers.customer_phone,
    customers.account_balance,
    customers.market_segment,
    nations.nation_key,
    nations.nation_name,
    regions.region_key,
    regions.region_name,
    customers.customer_comment
from customers
left join nations on customers.nation_key = nations.nation_key
left join regions on nations.region_key = regions.region_key


