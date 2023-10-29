select 
    'weekday_id',
    'weekday_name'
from {{ref("weekdays")}}
where weekday_id > 8