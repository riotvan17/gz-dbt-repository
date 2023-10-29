SELECT *
FROM {{ref('stg_raw__adwords')}}
UNION ALL 
SELECT *
FROM {{ref('stg_raw__bing')}}
UNION ALL 
SELECT *
FROM {{ref('stg_raw__criteo')}}
UNION ALL 
SELECT *
FROM {{ref('stg_raw__facebook')}}


---------------

- name: int_campaigns
    description: data from all campaigns
    tests:
      - unique:
          column_name: "(campaign_key || '-' || date_date)"
      - not_null:
          column_name:
            - paid_source
            - campaign_name

----------------

SELECT
    date_date,
    SUM(ads_cost) as ads_cost,
    SUM(impression) as ads_impression,
    SUM(click) as ads_clicks
FROM {{ ref("int_campaigns") }}
GROUP BY date_date
ORDER BY date_date DESC

--------------

SELECT 
	date_date,
	operational_margin - ads_cost AS ads_margin,
	ROUND(average_basket,2) AS average_basket
	operational_margin,
	ads_cost,
	ads_impressionn,
	ads_clicks,
	quantity,
	revenue,
	purchase_cost,
	margin,
	shipping_fee,
	logcost,
	ship_cost
FROM {{ ref('int_campaigns_day') }}
FULL OUTER JOIN {{ ref('finance_days') }} 
	USING (date_date)
ORDER BY date_date DESC


-----------------

models:
	- name: finance_campaigns_day
    description: global kpi
    columns:
      - name: ads_margin
        description: operational_margin - ads_cost
      - name: average_basket
        description: average basket cost for each day 
    tests:
      - not_null:
          column_name:
            - ads_margin
            - average_basket

----------------

SELECT 
	date_trunc(date_date, MONTH) AS datemonth, 
	SUM(operational_margin - ads_cost) AS ads_margin,
	ROUND(SUM(average_basket),2) AS average_basket,
	SUM(operational_margin) AS operational_margin,
	SUM(ads_cost) AS ads_cost,
	SUM(ads_impression) AS ads_impression,
	SUM(ads_clicks) AS ads_clicks,
	SUM(quantity) AS quantity,
	SUM(revenue) AS revenue,
	SUM(purchase_cost) AS purchase_cost,
	SUM(margin) AS margin,
	SUM(shipping_fee) AS shipping_fee,
	SUM(logcost) AS logcost,
	SUM(ship_cost) AS ship_cost,
FROM {{ ref('int_campaigns_day') }}
FULL OUTER JOIN {{ ref('finance_days') }} 
	USING (date_date)
GROUP BY datemonth
ORDER BY datemonth desc


-----------

