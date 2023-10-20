with 

source as (

    select * from {{ source('raw_data_circle', 'raw_cc_sales') }}

),

renamed as (

    select
        date_date,
        product_id,
        quantity

    from source

)

select * from renamed
