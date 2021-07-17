select customer_id,created_at from paytm_users
where access_token is not NULL
and created_at >= '2020-06-01';