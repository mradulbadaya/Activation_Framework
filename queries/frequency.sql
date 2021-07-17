SELECT a.id, b.frequency
from sf_leads a 
left join sf_lead_spot_details b 
on a.id = b.lead_spot_id
where a.created_at >= '2020-06-01';