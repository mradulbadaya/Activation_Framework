select customer_id, order_date,
timezone('Asia/Kolkata'::text,
				to_timestamp(pickup_time::double precision)) as order_time,
				vehicle_id,
				driver_rating,
				is_delayed,
				timezone('Asia/Kolkata'::text,
				to_timestamp(trip_started_time::double precision)) as trip_started_time,
					case when payment_mode = 0 then 'Cash'
	when payment_mode = 1 then 'Online'
	when payment_mode = 2 then 'Paytm' 
	when payment_mode = 3 then 'Instamojo' 
	when payment_mode = 4 then 'Rajorpay' end as payment_mode,
				order_id

from 
completed_spot_orders_mv
 where customer_id IN (`customers_list`)
;