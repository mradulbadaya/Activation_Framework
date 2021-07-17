with minimum_rating as ( 
select customer_id,min(driver_rating) as minimum_rating
from ( 
SELECT customer_id,  driver_rating,ROW_NUMBER() over (partition by customer_id order by pickup_time asc) as rank
from orders
where status = 4
and customer_id IN (`customers_list`)
)x 
where rank <= 2
group by customer_id
)
SELECT customer_id, case when minimum_rating = 1 or minimum_rating = 2 then 'Yes' ELSE 'N0' END AS poor_rating
from minimum_rating;