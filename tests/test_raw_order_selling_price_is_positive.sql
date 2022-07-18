with 
orders as (
    
select * from {{ ref('raw_order') }}
)

select 
    orderid, 
    sum(ordercostsellingprice) as total_sp
from orders
group by orderid
having total_sp<0