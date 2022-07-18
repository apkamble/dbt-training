
SELECT 
-- FROM raw orders
o.orderid, 
o.orderdate, 
o.shipdate, 
o.shipmode, 
o.ordercostsellingprice - o.ordercustprice as orderprofit,

-- from raw customer
c.customername, 
c.segment, 
c.country,

-- from raw product
p.productid,
p.category,
p.productname,
p.subcategory
from {{ ref('raw_order') }} as o
left join {{ ref('raw_customer') }} as c
on o.customerid = c.customerid
left join {{ ref('raw_product') }} as p 
on o.productid = p.productid