select 
date_date, 
count(orders_id) as nb_transactions,
round(sum(revenue), 0) as revenue,
sum(revenue)/count(orders_id) as avg_basket,
sum(margin) as margin,
sum(operational_margin) as operational_margin
from {{ref("int_orders_operational")}}
GROUP BY date_date
ORDER by date_date desc