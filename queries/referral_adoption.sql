SELECT b.id as customer_id, b.referral_code,a.referred_by_code
from customers a 
inner join (select id, referral_code from customers where created_at >= '2020-06-01') b 
on a.referred_by_code = b.referral_code;