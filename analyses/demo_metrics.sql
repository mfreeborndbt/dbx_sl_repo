-- Demo Queries for TPC-H Analysis
-- Run these queries to show interesting metrics

-- 1. Revenue by Region
select
    c.region_name,
    count(distinct oli.order_key) as total_orders,
    sum(oli.final_price) as total_revenue,
    avg(oli.final_price) as avg_order_value,
    sum(oli.quantity) as total_quantity
from {{ ref('order_line_items') }} oli
join {{ ref('dim_customers') }} c on oli.customer_key = c.customer_key
group by c.region_name
order by total_revenue desc;

-- 2. Customer Segments Performance
select
    c.market_segment,
    count(distinct c.customer_key) as customer_count,
    avg(c.account_balance) as avg_account_balance,
    count(distinct oli.order_key) as total_orders,
    sum(oli.final_price) as total_revenue
from {{ ref('dim_customers') }} c
left join {{ ref('order_line_items') }} oli on c.customer_key = oli.customer_key
group by c.market_segment
order by total_revenue desc;

-- 3. Monthly Revenue Trends
select
    date_trunc('month', oli.order_date) as order_month,
    count(distinct oli.order_key) as total_orders,
    sum(oli.final_price) as total_revenue,
    sum(oli.quantity) as items_sold,
    avg(oli.discount) as avg_discount_rate
from {{ ref('order_line_items') }} oli
where oli.order_date >= '2024-01-01'
group by date_trunc('month', oli.order_date)
order by order_month;

-- 4. Top Nations by Revenue
select
    c.nation_name,
    c.region_name,
    count(distinct c.customer_key) as customer_count,
    sum(oli.final_price) as total_revenue,
    avg(oli.final_price) as avg_order_value
from {{ ref('dim_customers') }} c
join {{ ref('order_line_items') }} oli on c.customer_key = oli.customer_key
group by c.nation_name, c.region_name
order by total_revenue desc
limit 10;

-- 5. Order Status Summary
select
    oli.order_status,
    count(distinct oli.order_key) as order_count,
    sum(oli.final_price) as total_revenue,
    avg(oli.final_price) as avg_order_value
from {{ ref('order_line_items') }} oli
group by oli.order_status
order by order_count desc;

