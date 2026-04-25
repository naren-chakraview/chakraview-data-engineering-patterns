{{
  config(
    materialized='table',
    tags=['daily', 'finance']
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

summary as (
    select
        cast(placed_at as date)    as order_date,
        status,
        count(*)                   as order_count,
        sum(amount_usd)            as total_revenue_usd,
        avg(amount_usd)            as avg_order_value_usd,
        min(placed_at)             as first_order_at,
        max(placed_at)             as last_order_at
    from orders
    group by 1, 2
)

select * from summary
order by order_date desc, status
