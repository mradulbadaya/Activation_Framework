SELECT
	customer_id,CASE 
		WHEN vehicle_type ILIKE '%2 wheeler%' THEN
			'2 wheeler'
		ELSE
			'LCV'
		END AS first_order_vehicle_type
FROM (
	SELECT
		a.customer_id,
		a.order_id,
		a.vehicle_id,
		a.order_date,
		b.vehicle_type,
		ROW_NUMBER() OVER (PARTITION BY a.customer_id ORDER BY a.order_date ASC) AS rank
	FROM
		completed_spot_orders a
	LEFT JOIN vehicles b ON a.vehicle_id = b.id
WHERE
	a.customer_id  IN (`customers_list`) )x
WHERE
	rank = 1;
 