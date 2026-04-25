with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        order_id::varchar          as order_id,
        customer_id::varchar       as customer_id,
        status::varchar            as status,
        amount_cents::integer      as amount_cents,
        amount_cents / 100.0       as amount_usd,
        placed_at::timestamp       as placed_at,
        updated_at::timestamp      as updated_at
    from source
    where order_id is not null
)

select * from renamed
