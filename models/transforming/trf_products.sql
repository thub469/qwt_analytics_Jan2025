{{config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA','TRANSFORMING'))}}
/*{{config(materialized = 'table', schema = 'transforming',transient = false,post_hook = 'create table transforming.trf_products_test clone transforming.trf_products')}}*/
select
p.productid,
p.productname,
s.companyname as suppliercompany,
s.contactname as suppliercontact,
s.city as suppliercity,
s.country as suppliercountry,
c.categoryname,
p.unitcost,
p.unitsinstock,
p.unitsonorder,
p.unitprice - p.unitcost as profit,
iff(p.unitsonorder > unitsinstock ,'Not Available','Avaliable') 
as prodcutavailability
from
{{ref('stg_products')}} as p left join {{ref('trf_suppliers')}} as s
on p.supplierid = s.supplierid left join {{ref('lkp_categories')}} as c
on p.categoryid = c.categoryid






