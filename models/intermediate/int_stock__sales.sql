select product_id, date_date, quantity, forecast_stock, stock, price
FROM {{ref("stg_raw_data_circle__raw_cc_sales")}}
JOIN {{ref("stg_raw_data_circle__raw_cc_stock")}}
USING (product_id)