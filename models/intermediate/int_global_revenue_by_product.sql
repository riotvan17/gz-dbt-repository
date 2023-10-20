SELECT * , avg_price * quantity as estimated_revenue
FROM {{ref("int_stock_sales_by_product")}}
ORDER BY estimated_revenue desc
