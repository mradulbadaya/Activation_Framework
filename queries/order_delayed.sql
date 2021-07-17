with delayed as (
select * from (
SELECT customer_id,
         case when is_delayed = TRUE then 1 else 0 end as is_delayed,
         ROW_NUMBER() over (partition by customer_id order by pickup_time asc) as rank
FROM completed_spot_orders 
WHERE 
customer_id IN (`customers_list`)
)x
where rank <= 2
)
select customer_id,max(is_delayed) as is_delayed
from delayed
group by customer_id
;

