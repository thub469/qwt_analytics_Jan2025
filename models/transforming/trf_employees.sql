/*{{config(materialized = 'table',schema = 'transforming')}}

select 
emp.empid,
emp.lastname,
emp.firstname,
emp.title,
emp.extension,
emp.hiredate,
emp.yearsalary,
IFF(mgr.firstname IS NULL,emp.firstname,mgr.firstname) as managername,
IFF(mgr.title is null,emp.title,mgr.title) as managertitle,
ofc.address,
ofc.city,
ofc.country
from {{ref('stg_employee')}} as emp left join  {{ref('stg_employee')}} as mgr 
on emp.reportsto= mgr.empid
left join  {{ref('stg_office')}} as ofc on ofc.office = emp.office */

{{config(materialized = 'table', schema = env_var('DBT_TRANSFORMSCHEMA','TRANSFORMING'))}}
 
with recursive managers (indent, officeid, employeeid, managerid, employeetitle, managertitle)
as
(
    select ' ' as indent, office as officeid, empid as employeeid, reportsto as managerid, title as employeetitle, title as managertitle
    from
    {{ref('stg_employee')}}
    where title = 'President'
 
    union all
 
    select indent || '* ', employees.office as officeid, employees.empid as employeeid, employees.reportsto as managerid, employees.title
    as employeetitle, mgr.title as maangertitle
 
    from
 
    {{ref("stg_employee")}} as employees join managers
    on employees.reportsto = managers.employeeid join {{ref('stg_employee')}} as mgr
    on employees.reportsto = mgr.empid
)
,
 
offices as
(
    select office,
           address,
           city,
           country
    from
    {{ref('stg_office')}}
)
 
select indent || employeetitle as title, managers.officeid, offices.address, offices.city, offices.country, employeeid, managerid, managertitle
from managers join offices on managers.officeid = offices.office
 
 