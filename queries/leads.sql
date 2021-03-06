WITH date_params AS (
	SELECT
		'`start_date`'::DATE AS START_date,
		'`end_date`'::DATE AS END_date
),
hh_leads AS (
	SELECT
		a.id,
		((a.sf_created_at AT TIME zone 'utc') AT TIME zone 'asia/kolkata') AS sf_created_at,
		b.lead_spot_id,
		b.first_form_source,
		b.lead_source,
		timezone('Asia/Kolkata'::text,
			to_timestamp(b.first_order_time::double precision)) AS first_order_date,
		timezone('Asia/Kolkata'::text,
			to_timestamp(b.second_order_time::double precision)) AS second_order_date,
		timezone('Asia/Kolkata'::text,
			to_timestamp(b.third_order_time::double precision)) AS third_order_date,
		c.customer_id,
		c. "number" AS phone_number
	FROM
		sf_leads a
		INNER JOIN sf_lead_spot_details b ON a.id = b.lead_spot_id
		INNER JOIN sf_lead_spot_detail_phones c ON b.id = c.detail_id
	WHERE (a.record_type = 'Spot'
		OR(a.salesforce_id LIKE 'a0R%'
			AND a.record_type IS NULL))
	AND ((a.sf_created_at AT TIME zone 'utc') AT TIME zone 'asia/kolkata') BETWEEN(
		SELECT
			START_date FROM date_params)
	AND(
		SELECT
			END_date FROM date_params)
)
select *
from hh_leads
where first_order_date is null or (first_order_date >= sf_created_at)
;