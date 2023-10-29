{% macro margin_percent(revenue, purchase_cost, precision=2) %}
   ROUND( SAFE_DIVIDE( ({{ revenue }} - {{ purchase_cost }}) , {{ revenue }} ) , {{ precision }})
{% endmacro %}

------------

{{ margin_percent('s.revenue', 's.quantity*CAST(p.purchSE_PRICE AS FLOAT64)') }}

-----------

{{
 dbt_utils.union_relations(
 relations=[ref('stg_raw__adwords'), ref('stg_raw__bing'), ref('stg_raw__criteo'), ref('stg_raw__facebook')]
 }}

----------
