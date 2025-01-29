{{config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA','TRANSFORMING'))}}
 
select
GET(XMLGET(Supplier, 'SupplierID'), '$') as supplierid,
GET(XMLGET(Supplier, 'CompanyName'), '$')::varchar as companyname,
GET(XMLGET(Supplier, 'ContactName'), '$')::varchar as contactname,
GET(XMLGET(Supplier, 'Address'), '$')::varchar  as address,
GET(XMLGET(Supplier, 'City'), '$')::varchar  as city,
GET(XMLGET(Supplier, 'PostalCode'), '$')::varchar  as postalcode,
GET(XMLGET(Supplier, 'Country'), '$')::varchar  as country,
GET(XMLGET(Supplier, 'Phone'), '$')::varchar  as phone,
GET(XMLGET(Supplier, 'Fax'), '$')::varchar  as fax,
from
{{ref('stg_suppliers')}}




