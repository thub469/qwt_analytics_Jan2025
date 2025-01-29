{{ config(materialized = 'incremental',unique_key = ['orderid','lineno'])}}

select 
od.*, o.orderdate
from
{{source('qwt_raw','raw_orders')}} o inner join
{{source('qwt_raw','raw_orderdetails')}} od on o.orderid = od.orderid

{% if is_incremental() %}
where o.orderdate > (select max(orderdate) from {{this}})
{% endif %}
