SELECT  
  Parcel_id AS parcel_id,
  Parcel_tracking AS parcel_tracking,
  Transporter AS transporter,
  Priority AS priority, 
  PARSE_DATE("%B %d, %Y", Date_purCHase) AS date_purchase,
  PARSE_DATE("%B %d, %Y", Date_sHIpping) AS date_shipping,
  PARSE_DATE("%B %d, %Y", DATE_delivery) AS date_delivery,
  PARSE_DATE("%B %d, %Y", DaTeCANcelled) AS date_cancelled,
FROM `my-project.raw_data_circle.raw_cc_parcel`

----

SELECT  
  ParCEL_id AS parcel_id,
  Model_mAME AS model_name,
  QUANTITY AS qty
FROM `my-project.raw_data_circle.raw_cc_parcel_product`

---

WITH nb_products_parcel AS (
	SELECT
		parcel_id
		,SUM(qty) AS qty
		,COUNT(DISTINCT model_name) AS nb_models
	FROM `parcel_dbt_dev_staging.stg_cc_parcel_product` 
	GROUP BY parcel_id
)

SELECT
  ### Key ###
  parcel_id
  ###########
  -- parcel infos
  ,parcel_tracking
  ,transporter
  ,priority
  -- date --
  ,date_purchase
  ,date_shipping
  ,date_delivery
  ,date_cancelled
  -- month --
  ,EXTRACT(MONTH FROM date_purchase) AS month_purchase
  -- status -- 
  ,CASE
    WHEN date_cancelled IS NOT NULL THEN 'Cancelled'
    WHEN date_shipping IS NULL THEN 'In Progress'
    WHEN date_delivery IS NULL THEN 'In Transit'
    WHEN date_delivery IS NOT NULL THEN 'Delivered'
    ELSE NULL
  END AS status
  -- time --
  ,DATE_DIFF(date_shipping,date_purchase,DAY) AS expedition_time
  ,DATE_DIFF(date_delivery,date_shipping,DAY) AS transport_time
  ,DATE_DIFF(date_delivery,date_purchase,DAY) AS delivery_time
  -- delay
  ,IF(date_delivery IS NULL,NULL,IF(DATE_DIFF(date_delivery,date_purchase,DAY)>5,1,0)) AS delay
	-- Metrics --
	,qty
	,nb_models
FROM `parcel_dbt_dev_staging.stg_cc_parcel`
LEFT JOIN nb_products_parcel USING (parcel_id)

---

WITH nb_products_parcel AS (
	SELECT
		parcel_id
    ,COUNT(DISTINCT(model_name)) AS nb_models
		,SUM(qty) AS qty
	FROM `parcel_dbt_dev_staging.stg_cc_parcel_product`
	GROUP BY parcel_id
)

SELECT
  ### Key ###
  parcel_id
  ###########
  -- parcel infos
  ,parcel_tracking
  ,transporter
  ,priority
  -- date --
  ,date_purchase
  ,date_shipping
  ,date_delivery
  ,date_cancelled
  -- month --
  ,EXTRACT(MONTH FROM date_purchase) AS month_purchase
  -- status -- 
  ,CASE
    WHEN date_cancelled IS NOT NULL THEN 'Cancelled'
    WHEN date_shipping IS NULL THEN 'In Progress'
    WHEN date_delivery IS NULL THEN 'In Transit'
    WHEN date_delivery IS NOT NULL THEN 'Delivered'
    ELSE NULL
  END AS status
  -- time --
  ,DATE_DIFF(date_shipping,date_purchase,DAY) AS expedition_time
  ,DATE_DIFF(date_delivery,date_shipping,DAY) AS transport_time
  ,DATE_DIFF(date_delivery,date_purchase,DAY) AS delivery_time
  -- delay
  ,IF(date_delivery IS NULL,NULL,IF(DATE_DIFF(date_delivery,date_purchase,DAY)>5,1,0)) AS delay
	-- Metrics --
	,qty
  , nb_models
FROM `parcel_dbt_dev_staging.stg_cc_parcel`
LEFT JOIN nb_products_parcel USING (parcel_id)