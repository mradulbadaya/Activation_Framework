
connect_to_db <- function(source) {
  drv <- dbDriver("PostgreSQL")
  source("~/Documents/connections.R")
  flog.info(fn$identity("Connecting to `source`"))
  if (source == "oms") {
    connection <- dbConnect(
      drv,
      host = oms_host,
      port = oms_port,
      user = oms_user,
      password = oms_pwd,
      dbname = oms_db
    )
  } else if (source == "sfms") {
    connection <- dbConnect(
      drv,
      host = sf_host,
      port = sf_port,
      user = sf_user,
      password = sf_pwd,
      dbname = sf_db
    )
  }else if (source == "redshift") {
    connection <- dbConnect(
      drv,
      host = redshift_host,
      port = redshift_port,
      user = redshift_user,
      password = redshift_pwd,
      dbname = redshift_db
    )
  }
  return(connection)
}

run_query <- function(source, query_statement) {
  # Runs a Query Statement on the connection source
  # Output: Query Data
  # Define Connection from Source
  connection <- connect_to_db(source)
  # Get Query Data
  query_data <- dbGetQuery(
    conn = connection,
    statement = query_statement
  )
  # Disconnect Connection
  dbDisconnect(conn = connection)
  # Return Query Data
  
  return(query_data)
}



library("RPostgreSQL")
library("gsubfn")
library("futile.logger")
library("readr")
library("dplyr")
library("lubridate")
library("googlesheets4")
library("data.table")



fetch_hh_leads_details <- function(start_date, end_date) {
  query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/leads.sql"))
  hh_leads_details <- run_query(source = "sfms", query_statement = query)
  hh_leads_with_customer_id <- hh_leads_details %>%
    filter(!is.na(customer_id)) %>%
    distinct(customer_id, .keep_all = TRUE) %>%
    mutate(whether_activated= ifelse(difftime( third_order_date, sf_created_at,units = "days")<= 35 , 1,0 ))
  return(hh_leads_with_customer_id)
}

hh_leads_1 <- fetch_hh_leads_details("2020-06-01", "2020-07-01")
hh_leads_2 <- fetch_hh_leads_details("2020-07-01", "2020-08-01")
hh_leads_3 <- fetch_hh_leads_details("2020-08-01", "2020-09-01")
hh_leads_4 <- fetch_hh_leads_details("2020-09-01", "2020-10-01")
hh_leads_5 <- fetch_hh_leads_details("2020-10-01", "2020-11-01")
hh_leads_6 <- fetch_hh_leads_details("2020-11-01", "2020-12-01")
hh_leads_7 <- fetch_hh_leads_details("2020-12-01", "2021-01-01")
hh_leads_8 <- fetch_hh_leads_details("2021-01-01", "2021-02-01")
hh_leads_9 <- fetch_hh_leads_details("2021-02-01", "2021-03-01")
hh_leads_10 <- fetch_hh_leads_details("2021-03-01", "2021-04-01")
hh_leads_11 <- fetch_hh_leads_details("2021-04-01", "2021-05-01")
hh_leads_12 <- fetch_hh_leads_details("2021-05-01", "2021-06-01")

# 
# hh_leads_1 <- read.csv("~/Documents/Activation Analysis/leads_1.csv")
# hh_leads_2 <- read.csv("~/Documents/Activation Analysis/leads_2.csv")
# hh_leads_3 <- read.csv("~/Documents/Activation Analysis/leads_3.csv")
# hh_leads_4 <- read.csv("~/Documents/Activation Analysis/leads_4.csv")
# hh_leads_5 <- read.csv("~/Documents/Activation Analysis/leads_5.csv")
# hh_leads_6 <- read.csv("~/Documents/Activation Analysis/leads_6.csv")
# hh_leads_7 <- read.csv("~/Documents/Activation Analysis/leads_7.csv")
# hh_leads_8 <- read.csv("~/Documents/Activation Analysis/leads_8.csv")
# # hh_leads_9 <- read.csv("~/Documents/Activation Analysis/leads_9.csv")
# # hh_leads_10 <- read.csv("~/Documents/Activation Analysis/leads_10.csv")
# # hh_leads_11 <- read.csv("~/Documents/Activation Analysis/leads_11.csv")
# # hh_leads_12 <- read.csv("~/Documents/Activation Analysis/leads_12.csv")

hh_leads = rbind(hh_leads_1,hh_leads_2,hh_leads_3,hh_leads_4,hh_leads_5,hh_leads_6,hh_leads_7,hh_leads_8)
# 
# hh_leads$sf_created_at =  as.Date(hh_leads$sf_created_at,"%d/%m/%y")
# hh_leads$first_order_date =  as.Date(hh_leads$first_order_date,"%d/%m/%y")
# hh_leads$second_order_date =  as.Date(hh_leads$first_order_date,"%d/%m/%y")
# hh_leads$third_order_date =  as.Date(hh_leads$first_order_date,"%d/%m/%y")

# hh_leads = hh_leads %>%
#   filter(!is.na(customer_id)) %>%
#   distinct(customer_id, .keep_all = TRUE) %>%
#   mutate(whether_activated= ifelse(difftime( as.Date(third_order_date), as.Date(sf_created_at),units = "days")<= 35 , 1,0 ))
# 
# test = hh_leads_import_1 %>% filter(as.Date(sf_created_at,"%d/%m/%y") >= "2020-08-01" & as.Date(sf_created_at,"%d/%m/%y") <= "2020-09-01")
# 
# as.Date(hh_leads_import_1$sf_created_at,"%d/%m/%y")

hh_leads = rbind(hh_leads_1,hh_leads_2,hh_leads_3,hh_leads_4,hh_leads_5,hh_leads_6,hh_leads_7,hh_leads_8,hh_leads_9,hh_leads_10,hh_leads_11,hh_leads_12)
# hh_leads_unuique = unique(hh_leads)

# hh_leads_export_1 =  hh_leads %>% filter(sf_created_at >= "2020-06-01" & sf_created_at <= "2020-09-01")
# hh_leads_export_2 =  hh_leads %>% filter(sf_created_at >= "2020-09-01" & sf_created_at <= "2020-12-01")
# hh_leads_export_3 =  hh_leads %>% filter(sf_created_at >= "2020-12-01" & sf_created_at <= "2021-03-01")
# hh_leads_export_4 =  hh_leads %>% filter(sf_created_at >= "2021-03-01" & sf_created_at <= "2021-06-01")
# 
# write.csv(hh_leads_export_1,"~/Documents/Activation Analysis/queries/hh_leads_export_1.csv")
# write.csv(hh_leads_export_2,"~/Documents/Activation Analysis/queries/hh_leads_export_2.csv")
# write.csv(hh_leads_export_3,"~/Documents/Activation Analysis/queries/hh_leads_export_3.csv")
# write.csv(hh_leads_export_4,"~/Documents/Activation Analysis/queries/hh_leads_export_4.csv")
# 
# hh_leads_import_1 = read.csv("~/Documents/Activation Analysis/queries/hh_leads_export_1.csv")
# hh_leads_import_2 = read.csv("~/Documents/Activation Analysis/queries/hh_leads_export_2.csv")
# hh_leads_import_3 = read.csv("~/Documents/Activation Analysis/queries/hh_leads_export_3.csv")
# hh_leads_import_4 = read.csv("~/Documents/Activation Analysis/queries/hh_leads_export_4.csv")
# 
# hh_leads = rbind(hh_leads_import_1,hh_leads_import_2,hh_leads_import_3,hh_leads_import_4)
## email verification
fetch_email_verification_status <- function(hh_leads){
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/email_verification.sql"))
  email_verification <- run_query(source = 'oms',query_statement = query)
  
  return(email_verification)
}

fetch_email_verification_status_hh_leads <- function(hh_leads) {
  email <- fetch_email_verification_status(hh_leads)
  leads_with_email_status <- hh_leads %>% left_join(email)
  leads_email_verification_status <- leads_with_email_status %>% mutate(whether_verified = case_when((confirmed_at <= as.Date(sf_created_at)+14) ~ "verified", TRUE ~ "not_verified"))
  return(leads_email_verification_status)
}

email_verification <- fetch_email_verification_status_hh_leads(hh_leads)



fetch_wallet_recharge_details <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/wallet_recharge.sql"))
  wallet_recharge_details <- run_query(source = "oms", query_statement = query)
  return(wallet_recharge_details)
}

fetch_hh_leads_wallet_recharge_details <- function(email_verification) {
  wallet_recharge <- fetch_wallet_recharge_details(hh_leads)
  # get total number of recharges before entering hh
  total_recharges <- email_verification %>%
    left_join(wallet_recharge) %>%
    filter(recharged_at >= sf_created_at) %>%
    group_by(customer_id) %>%
    summarise(recharges = n())
  total_recharges_14_days <- email_verification %>%
    left_join(wallet_recharge) %>%
    filter(recharged_at <= as.Date(sf_created_at) + 14) %>%
    group_by(customer_id) %>%
    summarise(recharges_14_days = n())
  total_recharges_14_days_amount <- email_verification %>%
    left_join(wallet_recharge) %>%
    filter(recharged_at <= as.Date(sf_created_at) + 14) %>%
    group_by(customer_id) %>%
    summarise(recharges_14_days_amount = sum(received_amount))
  # defining wallet recharge as boolean asthese are hh leads ,they wont do it more than once or twice
  wallet_recharge_hh_leads <- email_verification %>% left_join(total_recharges, by = "customer_id")
  wallet_recharge_hh_leads <- wallet_recharge_hh_leads %>% left_join(total_recharges_14_days, by = "customer_id")
  wallet_recharge_hh_leads <- wallet_recharge_hh_leads %>% left_join(total_recharges_14_days_amount, by = "customer_id")
  whether_recharged_wallet <- wallet_recharge_hh_leads %>% mutate(whether_recharged_lifetime = case_when(is.na(recharges) ~ "not_recharged", TRUE ~ "recharged"))
  whether_recharged_wallet <- whether_recharged_wallet %>% mutate(whether_recharged_14_days= case_when(is.na(recharges_14_days) ~ "not_recharged", TRUE ~ "recharged"))
  
  return(whether_recharged_wallet)
}

wallet_recharge = fetch_hh_leads_wallet_recharge_details(email_verification)

# # leads source
# fetch_leads_source<- function(hh_leads) {
#   customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
#   query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/leads_source.sql"))
#   leads_source_data <- run_query(source = "sfms", query_statement = query)
#   
#   return(leads_source_data)
# }
# 
# fetch_hh_leads_source<- function(wallet_recharge) {
#   leads_source_data <- fetch_leads_source(hh_leads)
#   leads_with_source <- wallet_recharge %>% left_join(leads_source_data)
#   return(leads_with_source)
# }
# 
# hh_leads_source_data <- fetch_hh_leads_source(wallet_recharge)

hh_leads_source_data = wallet_recharge

## porter gold

fetch_gold_subscription <- function(hh_leads) {
  customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
  query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/porter_gold.sql"))
  gold_customers <- run_query(source = "oms", query_statement = query)
  
  return(gold_customers)
}

fetch_gold_details_by_hh_leads <- function(hh_leads_source_data) {
  gold_subscription <- fetch_gold_subscription(hh_leads)
  total_gold_bought_30_days <- hh_leads_source_data %>%
    left_join(gold_subscription) %>%
    filter(gold_subscribed_at <= as.Date(sf_created_at) + 30) %>%
    group_by(customer_id) %>%
    summarise(total_gold_subs_30_days = n())
  total_gold_bought <- hh_leads_source_data %>%
    left_join(gold_subscription) %>%
    filter(gold_subscribed_at >= as.Date(sf_created_at)) %>%
    group_by(customer_id) %>%
    summarise(total_gold_subs = n())
  hh_leads_with_gold <- hh_leads_source_data %>% left_join(total_gold_bought)
  hh_leads_with_gold <- hh_leads_with_gold %>% left_join(total_gold_bought_30_days)
  whether_bought_gold <- hh_leads_with_gold %>% mutate(whether_bought_gold = case_when(is.na(total_gold_subs) ~ "not bought", TRUE ~ "bought"))
  whether_bought_gold <- whether_bought_gold %>% mutate(whether_bought_gold_30_days = case_when(is.na(total_gold_subs_30_days) ~ "not bought", TRUE ~ "bought"))
  
  return(whether_bought_gold)
}

porter_gold <- fetch_gold_details_by_hh_leads(hh_leads_source_data)

rm(hh_leads)
completed_orders = read.csv("~/Documents/Activation Analysis/completed_orders.csv")
completed_orders = activation_final_output_business %>% inner_join(completed_orders, by = "customer_id")
completed_orders = completed_orders %>% select("customer_id","order_date",
                                               "pickup_time",
                                               "vehicle_id",
                                               "driver_rating",
                                               "trip_started_time",
                                               "payment_mode",
                                               "order_id",
                                               "delay_time")

completed_orders$vehicle_type = ifelse(completed_orders$vehicle_id == 97, "2 Wheeler", "LCV")
#write.csv(completed_orders,"~/Documents/Activation Analysis/completed_orders.csv",row.names = FALSE)

order_ranks =  completed_orders %>% 
  group_by(customer_id) %>%
  mutate(my_ranks = order(order(pickup_time, decreasing=FALSE)))

## completed orders
# fetch_completed_orders <- function(hh_leads){
#   customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
#   query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/completed_orders.sql"))
#   completed_orders <- run_query(source = 'oms',query_statement = query)
#   
#   return(completed_orders)
# }

fetch_completed_orders_by_hh_leads <- function(porter_gold) {
  #  completed_orders <- fetch_completed_orders(hh_leads)
  completed_orders_7_days <- porter_gold %>%
    left_join(completed_orders) %>%
    filter(order_date <= as.Date(sf_created_at) + 7) %>%
    group_by(customer_id) %>%
    summarise(total_completed_orders_7_days = n())
  completed_orders_14_days <- porter_gold %>%
    left_join(completed_orders) %>%
    filter(order_date <= as.Date(sf_created_at) + 14) %>%
    group_by(customer_id) %>%
    summarise(total_completed_orders_14_days = n())
  completed_orders_21_days <- porter_gold %>%
    left_join(completed_orders) %>%
    filter(order_date <= as.Date(sf_created_at) + 21) %>%
    group_by(customer_id) %>%
    summarise(total_completed_orders_21_days = n())
  completed_orders_28_days <- porter_gold %>%
    left_join(completed_orders) %>%
    filter(order_date <= as.Date(sf_created_at) + 28) %>%
    group_by(customer_id) %>%
    summarise(total_completed_orders_28_days = n())
  completed_orders_total <- porter_gold %>%
    left_join(completed_orders) %>%
    filter(order_date >= as.Date(sf_created_at))  %>%
    group_by(customer_id) %>%
    summarise(total_completed_orders_total= n())
  
  leads_with_completed_order = porter_gold %>% left_join(completed_orders_7_days)
  leads_with_completed_order = leads_with_completed_order %>% left_join(completed_orders_14_days)
  leads_with_completed_order = leads_with_completed_order %>% left_join(completed_orders_21_days)
  leads_with_completed_order = leads_with_completed_order %>% left_join(completed_orders_28_days)
  leads_with_completed_order = leads_with_completed_order %>% left_join(completed_orders_total)
  
  return(leads_with_completed_order)
}

total_completed_orders = fetch_completed_orders_by_hh_leads(porter_gold)

# fetch_first_orders_vehicle_type <- function(hh_leads){
#   customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
#   query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/first_order_vehicle_type.sql"))
#   first_order_vehicle_type <- run_query(source = 'oms',query_statement = query)
#   
#   return(first_order_vehicle_type)
# }


first_order_vehicle_type =  order_ranks %>%
  filter(my_ranks == 1)
first_order_vehicle_type = first_order_vehicle_type %>%
  select("customer_id","vehicle_type")

fetch_first_orders_vehicle_type_hh_leads <- function(total_completed_orders) {
  # first_orders_vehicle_type <- fetch_first_orders_vehicle_type(hh_leads)
  leads_with_first_orders_vehicle_type <- total_completed_orders %>% left_join(first_order_vehicle_type)
  return(leads_with_first_orders_vehicle_type)
}

first_order_vehicle_type <- fetch_first_orders_vehicle_type_hh_leads(total_completed_orders)
first_order_vehicle_type <- first_order_vehicle_type %>%
  rename(first_order_vehicle_type = vehicle_type)

# ## vehicle type
# fetch_vehicle_type <- function(hh_leads){
#   customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
#   query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/vehicle_type.sql"))
#   vehicle_type <- run_query(source = 'oms',query_statement = query)
#   
#   return(vehicle_type)
# }


fetch_vehicle_type_hh_leads <- function(first_order_vehicle_type) {
  #vehicle_type <- fetch_vehicle_type(hh_leads)
  LCV_orders <- first_order_vehicle_type %>%
    left_join(completed_orders) %>%
    filter(vehicle_type == 'LCV') %>%
    group_by(customer_id) %>%
    summarise(total_LCV_orders = n())
  Two_Wheeler_orders <- first_order_vehicle_type %>%
    left_join(completed_orders) %>%
    filter(vehicle_type == '2 Wheeler') %>%
    group_by(customer_id) %>%
    summarise(total_2_wheeler_orders = n())
  
  leads_with_vehicle_type = first_order_vehicle_type %>% left_join(LCV_orders)
  leads_with_vehicle_type = leads_with_vehicle_type %>% left_join(Two_Wheeler_orders)
  
  return(leads_with_vehicle_type)
}

vehicle_type <- fetch_vehicle_type_hh_leads(first_order_vehicle_type)

## trip rating

# fetch_trip_rating <- function(hh_leads) {
#   customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
#   query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/trip_rating.sql"))
#   ratings_data <- run_query(source = "oms", query_statement = query)
#   
#   return(ratings_data)
# }



ratings_data = order_ranks %>%
  filter(my_ranks <= 5) %>%
  group_by(customer_id)  %>%
  summarise(average_rating = mean(driver_rating,na.rm =T))
ratings_data$average_rating[is.na(ratings_data$average_rating)] = 'NA'

fetch_hh_trip_rating <- function(vehicle_type) {
  # ratings_data <- fetch_trip_rating(hh_leads)
  leads_with_trip_rating <- vehicle_type %>% left_join(ratings_data)
  return(leads_with_trip_rating)
}

average_ratings_data = fetch_hh_trip_rating(vehicle_type)

## poor trip rating
poor_ratings_data =  order_ranks  %>%
  filter(my_ranks <= 2)

poor_ratings_data$driver_rating[is.na(poor_ratings_data$driver_rating)] = 99

poor_ratings_data_1 = poor_ratings_data %>%
  group_by(customer_id) %>%
  summarise(minimum_rating = min(driver_rating,na.rm = T))

poor_ratings_data_1$whether_minimum_rating = ifelse(poor_ratings_data_1$minimum_rating == 1 | poor_ratings_data_1$minimum_rating == 2, 'Yes','No')

# fetch_poor_trip_rating <- function(hh_leads) {
#   customers_list <- paste("'", paste(hh_leads$customer_id, collapse = "','"), "'", sep = "")
#   query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/poor_rating_in_first_two_orders.sql"))
#   poor_ratings_data <- run_query(source = "oms", query_statement = query)
#   
#   return(poor_ratings_data)
# }

fetch_hh_poor_trip_rating <- function(average_ratings_data) {
  # poor_ratings_data <- fetch_poor_trip_rating(hh_leads)
  leads_with_poor_trip_rating <- average_ratings_data %>% left_join(poor_ratings_data_1)
  return(leads_with_poor_trip_rating)
}

poor_ratings_data = fetch_hh_poor_trip_rating(average_ratings_data)

poor_ratings_data = read.csv("~/Documents/Activation Analysis/poor_ratings_data.csv")
rm(completed_orders_1)
order_ranks_retention =  completed_orders %>% 
  group_by(customer_id) %>%
  mutate(my_ranks = order(order(pickup_time, decreasing=TRUE)))

# max_orders = order_ranks_retention %>%
#   filter(my_ranks == 1)
# max_orders =  max_orders %>%
#   select("customer_id","order_time")



# write.csv(completed_orders,"~/Documents/Activation Analysis/completed_orders.csv",row.names = FALSE)
# write.csv(poor_ratings_data,"~/Documents/Activation Analysis/poor_ratings_data.csv",row.names = FALSE)

first_order_payment_mode = order_ranks %>%
  filter(my_ranks == 1) %>%
  select("customer_id","payment_mode")

leads_with_first_order_payment_mode <- poor_ratings_data %>% left_join(first_order_payment_mode)

fetch_paytm_users <- function(leads_with_first_order_payment_mode){
  query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/paytm_users.sql"))
  paytm_users <- run_query(source = 'oms',query_statement = query)
  
  return(paytm_users)
}

fetch_email_verification_status_hh_leads <- function(leads_with_first_order_payment_mode) {
  paytm_users <- fetch_paytm_users(leads_with_first_order_payment_mode)
  leads_with_paytm_linked <- leads_with_first_order_payment_mode %>% left_join(paytm_users)
  leads_with_paytm_linked_14_days <- leads_with_paytm_linked %>% mutate(whether_paytm_linked_14_days = case_when((created_at <= as.Date(sf_created_at)+14) ~ "linked", TRUE ~ "not_linked"))
  return(leads_with_paytm_linked_14_days)
}

leads_with_paytm_linked <- fetch_email_verification_status_hh_leads(leads_with_first_order_payment_mode)
# write.csv(leads_with_paytm_linked,"~/Documents/Activation Analysis/leads_with_paytm_linked.csv")



completed_orders_1 = completed_orders %>% select("customer_id","order_date")
orders <- leads_with_paytm_linked %>% left_join(completed_orders_1)
orders <- orders %>% mutate(last_order_week =difftime( as.Date(order_date), as.Date(first_order_date),units = "days") )
orders <- orders %>% mutate(retention = ifelse(last_order_week > 0 & last_order_week <= 14,"less_than_14_days_retention",
                                               ifelse(last_order_week > 0 & last_order_week <= 28,"less_than_28_days_retention",
                                               ifelse(last_order_week > 35 & last_order_week <= 42,"6_week_retention",
                                                      ifelse(last_order_week > 77 & last_order_week <= 84,"12_week_retention",
                                                             ifelse(last_order_week > 161 & last_order_week <= 168,"24_week_retention" ,
                                                                    ifelse(last_order_week > 329 & last_order_week <= 336,"48_week_retention",'NA')))))))
# activation_final_output_business = activation_final_output_business %>% rename("x_8_week_retention" = "x48_week_retention")
# activation_final_output_business = activation_final_output_business[,!x_8_week_retention]
colnames(activation_final_output_business)
#rm(completed_orders_1)
# orders = orders %>% mutate(retention_1 = ifelse(retention == 'NA','No','Yes'))
# colnames(orders)
# leads_with_retention =  dcast(orders, customer_id ~ retention, value.var="retention_1")
# library("tidyr")
# leads_with_retention = spread(orders,retention,retention_1)
leads_with_retention <- orders %>%
  mutate(less_than_14_days_retention = ifelse(retention == 'less_than_14_days_retention', 1,0)) %>%
  mutate(less_than_28_days_retention = ifelse(retention == 'less_than_28_days_retention', 1,0)) %>%
  mutate(x6_week_retention = ifelse(retention == '6_week_retention', 1,0)) %>%
  mutate(x12_week_retention = ifelse(retention == '12_week_retention', 1,0)) %>%
  mutate(x24_week_retention = ifelse(retention == '24_week_retention', 1,0)) %>%
  mutate(x48_week_retention = ifelse(retention == '48_week_retention', 1,0))

leads_with_retention = leads_with_retention %>% group_by(customer_id) %>%
  summarise("less_than_14_days_retention" = max(less_than_14_days_retention),"less_than_28_days_retention" = max(less_than_28_days_retention),"6_week_retention" = max(x6_week_retention),
            "12_week_retention" = max(x12_week_retention),"24_week_retention" = max(x24_week_retention),
            "48_week_retention" = max(x48_week_retention)) 


leads_with_retention =     leads_with_paytm_linked %>% left_join(leads_with_retention)  

# leads_with_retention = read.csv("~/Documents/Activation Analysis/leads_with_retention")
# write.csv(leads_with_retention,"~/Documents/Activation Analysis/leads_with_retention")



delayed_orders <- order_ranks %>%
  filter(my_ranks <= 2)

delayed_orders_1 <- delayed_orders %>%
  mutate(delay_time = ifelse(delay_time > 15,1,0))

delayed_orders_1 <- delayed_orders_1 %>% group_by(customer_id)  %>%
                  summarise(is_delayed = max(delay_time))

leads_with_delayed_order <- leads_with_retention %>% left_join(delayed_orders_1)

cancelled_orders = read.csv("~/Documents/Activation Analysis/cancelled_orders.csv")
cancelled_orders = cancelled_orders %>% 
  filter(cancel_reason_id == '156' |cancel_reason_id == '155' |cancel_reason_id == '130' |cancel_reason_id == '83' |
           cancel_reason_id == '73' |cancel_reason_id == '72' |cancel_reason_id == '63' |cancel_reason_id == '59' |
           cancel_reason_id == '58' |cancel_reason_id == '50' )

cancelled_orders_1 = cancelled_orders %>% group_by(customer_id) %>% summarise(cancelled_order_count = n())

leads_with_cancelled_orders = leads_with_delayed_order %>% left_join(cancelled_orders_1)
leads_with_cancelled_orders = leads_with_cancelled_orders %>% mutate_at("cancelled_order_count", ~ replace(., is.na(.), 0))
leads_with_cancelled_orders = leads_with_cancelled_orders %>%  mutate(whether_cancelled = case_when(cancelled_order_count == 0 ~ "not_cancelled", TRUE ~ "cancelled"))



cc_tickets = read.csv("~/Documents/Activation Analysis/cc_tickets.csv")
cc_tickets$customer_partner_phone = as.numeric(cc_tickets$customer_partner_phone)
cc_tickets = leads_with_cancelled_orders %>% inner_join(cc_tickets, by = c("phone_number" = "customer_partner_phone" ))
cc_tickets = cc_tickets %>% select("phone_number","created_at.y","crn","order_stage_v2")

cc_tickets <- cc_tickets %>%
  rename(cc_ticket_created_at= created_at.y)

first_order_data = order_ranks %>% filter(my_ranks ==1)
first_order_cc_ticket = first_order_data %>% inner_join(cc_tickets, by = c("order_id"= "crn"))
first_order_cc_ticket = first_order_cc_ticket %>% group_by(customer_id) %>% summarise(first_order_cc_count = n())
leads_with_first_order_cc_count = leads_with_cancelled_orders %>% left_join(first_order_cc_ticket)
leads_with_first_order_cc_count = leads_with_first_order_cc_count %>% mutate_at("first_order_cc_count", ~ replace(., is.na(.), 0))
leads_with_first_order_cc_count = leads_with_first_order_cc_count %>%  mutate(first_order_cc_count = case_when(first_order_cc_count == 0 ~ "No", TRUE ~ "Yes"))

cc_tickets_30_days = leads_with_first_order_cc_count %>% left_join(cc_tickets) %>% filter(cc_ticket_created_at <= as.Date(sf_created_at)+30)
cc_tickets_30_days = cc_tickets_30_days %>% group_by(customer_id) %>% summarise(cc_ticket_count = n())
leads_with_cc_ticket_count = leads_with_first_order_cc_count %>% left_join(cc_tickets_30_days)
leads_with_cc_ticket_count = leads_with_cc_ticket_count %>% mutate_at("cc_ticket_count", ~ replace(., is.na(.), 0))
leads_with_cc_ticket_count = leads_with_cc_ticket_count %>%  mutate(cc_ticket_30_days = case_when(cc_ticket_count == 0 ~ "No", TRUE ~ "Yes"))

# write.csv(leads_with_cc_ticket_count,"~/Documents/Activation Analysis/leads_with_cc_ticket_count.csv",row.names = FALSE)
# 
# rm(order_ranks)



fetch_referral <- function(leads_with_cc_ticket_count){
  query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/referral_adoption.sql"))
  referral_leads <- run_query(source = 'oms',query_statement = query)
  
  return(referral_leads)
}

fetch_referral_adoption_hh_leads <- function(leads_with_cc_ticket_count) {
  referral_leads <- fetch_referral(leads_with_cc_ticket_count)
  referral_leads <- referral_leads %>% group_by(customer_id) %>%
    summarise(referral_count= n())
  leads_with_referral_adoption <- leads_with_cc_ticket_count %>% left_join(referral_leads)
  leads_with_referral_adoption = leads_with_referral_adoption %>% mutate_at("referral_count", ~ replace(., is.na(.), 0))
  leads_with_referral_adoption = leads_with_referral_adoption %>%  mutate(whether_referral_adoption = case_when(referral_count == 0 ~ "No", TRUE ~ "Yes"))
  return(leads_with_referral_adoption)
}

leads_with_referral_adoption <- fetch_referral_adoption_hh_leads(leads_with_cc_ticket_count)

# write.csv(leads_with_referral_adoption,"~/Documents/Activation Analysis/leads_with_referral_adoption.csv",row.names = FALSE)
# leads_with_referral_adoption = read.csv("~/Documents/Activation Analysis/leads_with_referral_adoption.csv")

## customer frequency
fetch_customer_frequency <- function(leads_with_referral_adoption) {
  query <- fn$identity(read_file("~/Documents/Activation Analysis/queries/frequency.sql"))
  orders <- run_query(source = "sfms", query_statement = query)
  
  return(orders)
}

lead_with_frequency <- function(leads_with_referral_adoption) {
  frequency <- fetch_customer_frequency(leads_with_referral_adoption)
  lead_with_frequency<- leads_with_referral_adoption %>% left_join(frequency)
  return(lead_with_frequency)
}

leads_with_frequency = lead_with_frequency(leads_with_referral_adoption)
colnames(leads_with_frequency)
activation_final_output <- leads_with_frequency %>% select("customer_id","first_order_date","sf_created_at","whether_activated","frequency","whether_verified","payment_mode","whether_paytm_linked_14_days",
                                                           "whether_recharged_lifetime","whether_recharged_14_days","recharges_14_days_amount","first_form_source",
                                                           "lead_source","whether_referral_adoption","whether_bought_gold_30_days","whether_bought_gold","is_delayed",
                                                           "whether_cancelled","whether_minimum_rating","first_order_cc_count","cc_ticket_30_days","average_rating",
                                                           "first_order_vehicle_type","total_LCV_orders","total_2_wheeler_orders","X4_week_retention",
                                                           "X6_week_retention","X12_week_retention","X24_week_retention","X48_week_retention","total_completed_orders_7_days",
                                                           "total_completed_orders_14_days","total_completed_orders_21_days","total_completed_orders_28_days",
                                                           "total_completed_orders_total")
colSums(is.na(activation_final_output))
colnames(activation_final_output)
activation_final_output$whether_activated[is.na(activation_final_output$whether_activated)] = 0
activation_final_output$frequency[is.na(activation_final_output$frequency)] = 'NA'

# activation_final_output$recharges_14_days_amount[is.na(activation_final_output$recharges_14_days_amount)] = 0
activation_final_output$payment_mode[is.na(activation_final_output$payment_mode)] = 'NA'
activation_final_output$is_delayed[is.na(activation_final_output$is_delayed)] = 'NA'
activation_final_output$whether_minimum_rating[is.na(activation_final_output$whether_minimum_rating)] = 'NA'
activation_final_output$average_rating[is.na(activation_final_output$average_rating)] = 0
activation_final_output$first_order_vehicle_type[is.na(activation_final_output$first_order_vehicle_type)] = 'NA'
# activation_final_output$total_LCV_orders[is.na(activation_final_output$total_2_wheeler_orders)] = 0
# activation_final_output$total_2_wheeler_orders[is.na(activation_final_output$total_2_wheeler_orders)] = 0
activation_final_output$X4_week_retention[is.na(activation_final_output$X4_week_retention)] = 0
activation_final_output$X6_week_retention[is.na(activation_final_output$X6_week_retention)] = 0
activation_final_output$X12_week_retention[is.na(activation_final_output$X12_week_retention)] = 0
activation_final_output$X24_week_retention[is.na(activation_final_output$X24_week_retention)] = 0
activation_final_output$X48_week_retention[is.na(activation_final_output$X48_week_retention)] = 0
# activation_final_output$total_completed_orders_7_days[is.na(activation_final_output$total_completed_orders_7_days)] = 0
# activation_final_output$total_completed_orders_14_days[is.na(activation_final_output$total_completed_orders_14_days)] = 0
# activation_final_output$total_completed_orders_21_days[is.na(activation_final_output$total_completed_orders_21_days)] = 0
# activation_final_output$total_completed_orders_28_days[is.na(activation_final_output$total_completed_orders_28_days)] = 0
# activation_final_output$total_completed_orders_total[is.na(activation_final_output$total_completed_orders_total)] = 0


activation_final_output <- activation_final_output %>% mutate(recharges_14_days_amount_bucket = case_when(is.na(recharges_14_days_amount) == T ~ "never_recharged", recharges_14_days_amount < 1000 ~ "less_than_1000", TRUE ~ "more_than_and_equal_to_1000"))
activation_final_output <- activation_final_output %>% mutate(LCV_orders_bucket = case_when(is.na(total_LCV_orders) == T ~ "never_ordered", total_LCV_orders <= median(total_LCV_orders,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))
activation_final_output <- activation_final_output %>% mutate(total_2_wheeler_orders_bucket = case_when(is.na(total_2_wheeler_orders) == T ~ "never_ordered", total_2_wheeler_orders <= median(total_2_wheeler_orders,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))
activation_final_output <- activation_final_output %>% mutate(completed_orders_7_days_bucket = case_when(is.na(total_completed_orders_7_days) == T ~ "never_ordered", total_completed_orders_7_days <= median(total_completed_orders_7_days,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))
activation_final_output <- activation_final_output %>% mutate(completed_orders_14_days_bucket = case_when(is.na(total_completed_orders_14_days) == T ~ "never_ordered", total_completed_orders_14_days <= median(total_completed_orders_14_days,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))
activation_final_output <- activation_final_output %>% mutate(completed_orders_21_days_bucket = case_when(is.na(total_completed_orders_21_days) == T ~ "never_ordered", total_completed_orders_21_days <= median(total_completed_orders_21_days,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))
activation_final_output <- activation_final_output %>% mutate(completed_orders_28_days_bucket = case_when(is.na(total_completed_orders_28_days) == T ~ "never_ordered", total_completed_orders_28_days <= median(total_completed_orders_28_days,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))
activation_final_output <- activation_final_output %>% mutate(completed_orders_total_bucket = case_when(is.na(total_completed_orders_total) == T ~ "never_ordered", total_completed_orders_total <= median(total_completed_orders_total,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))

write.csv(activation_final_output,"~/Documents/Activation Analysis/activation_final_output_business_frequency.csv",row.names = FALSE)

activation_final_output_business = activation_final_output %>% filter(frequency == 'Business')
activation_final_output_business = activation_final_output_business %>% filter(as.Date(sf_created_at) >= '2020-07-01' & as.Date(sf_created_at) <= '2021-03-31' )

median(activation_final_output$recharges_14_days_amount_bucket)
table(activation_final_output$whether_activated,activation_final_output$whether_verified)
prop.table(table(activation_final_output[["whether_activated"]], activation_final_output[["completed_orders_total_bucket"]]), margin = 2) * 100



activation_final_output <- leads_with_retention %>% select("customer_id","whether_activated","frequency","whether_verified","payment_mode","whether_paytm_linked_14_days",
                                                              "whether_recharged_lifetime","whether_recharged_14_days","recharges_14_days_amount","recharges_14_days_amount_bucket","first_form_source",
                                                              "lead_source","whether_referral_adoption","whether_bought_gold_30_days","whether_bought_gold","is_delayed",
                                                              "whether_cancelled","whether_minimum_rating","first_order_cc_count","cc_ticket_30_days","average_rating",
                                                              "first_order_vehicle_type","total_LCV_orders","LCV_orders_bucket","total_2_wheeler_orders","total_2_wheeler_orders_bucket",
                                                              "less_than_14_days_retention", "less_than_28_days_retention","6_week_retention","12_week_retention","24_week_retention","48_week_retention","total_completed_orders_7_days","completed_orders_7_days_bucket",
                                                              "total_completed_orders_14_days","completed_orders_14_days_bucket","total_completed_orders_21_days","completed_orders_21_days_bucket",
                                                              "total_completed_orders_28_days","completed_orders_28_days_bucket", "total_completed_orders_total","completed_orders_total_bucket")


perform_chisquare_test <- function(data, column1, column2) {
  # Subset df for column1.not_null AND column2.not_null
  required_data <- data %>%
    filter(!is.na(column1)) %>%
    filter(!is.na(column2))
  # Prepare contingency table
  frequency_info <- table(required_data[[column1]], required_data[[column2]])
  # prepare proportion table
  proportion_info <- prop.table(table(required_data[[column1]], required_data[[column2]]), margin = 2)
  # Run statistical test
  metric_list <- list(
    "factor" = column2,
    "count_of_factors" = frequency_info,
    "proportion_of_factors" = proportion_info
  )
  return(metric_list)
}

perform_hh_leads_conversion_eda <- function(activation_final_output) {
  list_of_metrics <- list("frequency","whether_verified","payment_mode","whether_paytm_linked_14_days",
                          "whether_recharged_lifetime","whether_recharged_14_days","recharges_14_days_amount_bucket","first_form_source",
                          "lead_source","whether_referral_adoption","whether_bought_gold_30_days","whether_bought_gold","is_delayed",
                          "whether_cancelled","whether_minimum_rating","first_order_cc_count","cc_ticket_30_days","average_rating",
                          "first_order_vehicle_type","LCV_orders_bucket","total_2_wheeler_orders_bucket",
                          "less_than_14_days_retention","less_than_28_days_retention", "6_week_retention","12_week_retention","24_week_retention","48_week_retention","completed_orders_7_days_bucket",
                          "completed_orders_14_days_bucket","completed_orders_21_days_bucket"
                          ,"completed_orders_28_days_bucket","completed_orders_total_bucket")
  chi_square_result <- c()
  for (metric in list_of_metrics) {
    chi_square_result <- perform_chisquare_test(data = activation_final_output, column1 = "whether_activated", column2 = metric)
    print(chi_square_result)
  }
  return(chi_square_result)
}
perform_hh_leads_conversion_eda(activation_final_output)

activation_final_output_1 <- activation_final_output %>% filter(whether_activated == 1 & is_delayed == 'NA')
activation_final_output_2 = data.frame(activation_final_output_1[,1])

total_completed_orders_7_days = data.frame(activation_final_output[!is.na(activation_final_output$total_completed_orders_7_days),])
total_completed_orders_7_days = total_completed_orders_7_days %>% filter(whether_activated == 1)
median(total_completed_orders_7_days$total_completed_orders_7_days)
total_completed_orders_7_days <- total_completed_orders_7_days %>% mutate(completed_orders_7_days_bucket = case_when(is.na(total_completed_orders_7_days) == T ~ "never_ordered", total_completed_orders_7_days <= median(total_completed_orders_7_days,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))

table(total_completed_orders_7_days$whether_activated,total_completed_orders_7_days$completed_orders_7_days_bucket)

total_completed_orders_14_days = data.frame(activation_final_output[!is.na(activation_final_output$total_completed_orders_14_days),])
total_completed_orders_14_days = total_completed_orders_14_days %>% filter(whether_activated == 1)
total_completed_orders_14_days <- total_completed_orders_14_days %>% mutate(completed_orders_14_days_bucket = case_when(is.na(total_completed_orders_14_days) == T ~ "never_ordered", total_completed_orders_14_days <= median(total_completed_orders_14_days,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))
median(total_completed_orders_14_days$total_completed_orders_14_days)
table(total_completed_orders_14_days$whether_activated,total_completed_orders_14_days$completed_orders_14_days_bucket)


total_completed_orders_21_days = data.frame(activation_final_output[!is.na(activation_final_output$total_completed_orders_21_days),])
total_completed_orders_21_days = total_completed_orders_21_days %>% filter(whether_activated == 1)
total_completed_orders_21_days <- total_completed_orders_21_days %>% mutate(completed_orders_21_days_bucket = case_when(is.na(total_completed_orders_21_days) == T ~ "never_ordered", total_completed_orders_21_days <= median(total_completed_orders_21_days,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))
median(total_completed_orders_21_days$total_completed_orders_21_days)
table(total_completed_orders_21_days$whether_activated,total_completed_orders_21_days$completed_orders_21_days_bucket)



total_completed_orders_28_days = data.frame(activation_final_output[!is.na(activation_final_output$total_completed_orders_28_days),])
total_completed_orders_28_days = total_completed_orders_28_days %>% filter(whether_activated == 1)
total_completed_orders_28_days <- total_completed_orders_28_days %>% mutate(completed_orders_28_days_bucket = case_when(is.na(total_completed_orders_28_days) == T ~ "never_ordered", total_completed_orders_28_days <= median(total_completed_orders_28_days,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))
median(total_completed_orders_28_days$total_completed_orders_28_days)
table(total_completed_orders_28_days$whether_activated,total_completed_orders_28_days$completed_orders_28_days_bucket)


total_completed_orders_total = data.frame(activation_final_output[!is.na(activation_final_output$total_completed_orders_total),])
total_completed_orders_total = total_completed_orders_total %>% filter(whether_activated == 1)
total_completed_orders_total <- total_completed_orders_total %>% mutate(completed_orders_total_bucket = case_when(is.na(total_completed_orders_total) == T ~ "never_ordered", total_completed_orders_total <= median(total_completed_orders_total,na.rm=T) ~ "less_than_median_orders", TRUE ~ "more_than_median_orders"))
median(total_completed_orders_total$total_completed_orders_total)
table(total_completed_orders_total$whether_activated,total_completed_orders_total$completed_orders_total_bucket)


summary(total_completed_orders_7_days$total_completed_orders_7_day)
median(total_completed_orders_7_days$total_completed_orders_7_days)
median(total_completed_orders_14_days$total_completed_orders_14_days)
median(total_completed_orders_21_days$total_completed_orders_21_days)
median(total_completed_orders_28_days$total_completed_orders_28_days)
median(total_completed_orders_total$total_completed_orders_total)
summary(total_completed_orders_total$total_completed_orders_total)


