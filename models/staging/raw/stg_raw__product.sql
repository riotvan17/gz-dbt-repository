with 

source as (

    select * from {{ source('raw', 'product') }}

),

renamed as (

    select
        products_id,
        SAFE_CAST(purchse_price as int64) as purchase_price

    from source

)

select * from renamed
