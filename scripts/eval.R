# Setup Work Environment
# -------------

# Clean the worskspace
rm(list = ls())

# Loading Packages
library(KoboconnectR)
library(tidyverse)
library(janitor)
library(lubridate)
library(googlesheets4)

# Identify Data
# -------------

# Load the KoboToolbox credentials
USER_ID <- Sys.getenv(c("USER_ID"))
PASSWORD <- Sys.getenv(c("PASSWORD"))

# Login into Kobo
kobo_token <- get_kobo_token(url = "kf.kobotoolbox.org", uname = USER_ID, pwd = PASSWORD)

# Login into Google
gs4_auth()

# List of Kobo forms data to download
kobo_forms_list <- kobotools_api(url = "kf.kobotoolbox.org", simplified = T, uname = USER_ID, pwd = PASSWORD)

# Downloading Data
# -------------

# Select forms to download
id_market_vyapaaris <- kobo_forms_list[kobo_forms_list$name == "Market Entry Survey and Baseline - Vyapaaris", ]$asset
id_market_retailers <- kobo_forms_list[kobo_forms_list$name == "Market Entry Survey and Baseline - Retailers", ]$asset
id_pitching_vyapaaris <- kobo_forms_list[kobo_forms_list$name == "Pitching - Vyapaaris", ]$asset
id_onbaording_vyapaaris <- kobo_forms_list[kobo_forms_list$name == "Onboarding - Vyapaaris", ]$asset
id_pitching_onbaording_vyapaaris <- kobo_forms_list[kobo_forms_list$name == "Pitching and Onboarding - Vyapaaris", ]$asset

# Market Survey - Vyapaaris
data_market_vyapaaris <- 
  kobo_df_download(
    url = "kf.kobotoolbox.org",
    uname = USER_ID,
    pwd = PASSWORD,
    assetid = id_market_vyapaaris,
    all = "true",
    lang = "_xml",
    sleep = 5
  )

# Market Survey - Retailers
data_market_retailers <- 
  kobo_df_download(
    url = "kf.kobotoolbox.org",
    uname = USER_ID,
    pwd = PASSWORD,
    assetid = id_market_retailers,
    all = "true",
    lang = "_xml",
    sleep = 5
  )

# Pitching Data
data_pitching_vyapaaris <- 
  kobo_df_download(
    url = "kf.kobotoolbox.org",
    uname = USER_ID,
    pwd = PASSWORD,
    assetid = id_pitching_vyapaaris,
    all = "true",
    lang = "_xml",
    sleep = 5
  )

# Onboarding Data
data_onboarding_vyapaaris <- 
  kobo_df_download(
    url = "kf.kobotoolbox.org",
    uname = USER_ID,
    pwd = PASSWORD,
    assetid = id_onbaording_vyapaaris,
    all = "true",
    lang = "_xml",
    sleep = 5
  )

# Pitching and Onboarding Data << extra pitching data from old form >>
data_pitching_onboarding_vyapaaris <- 
  kobo_df_download(
    url = "kf.kobotoolbox.org",
    uname = USER_ID,
    pwd = PASSWORD,
    assetid = id_pitching_onbaording_vyapaaris,
    all = "true",
    lang = "_xml",
    sleep = 5
  )

# Verification Data
data_verification_vyapaaris <- read_sheet("https://docs.google.com/spreadsheets/d/1irc7ENnrQ7w1IZ4vcBU9pzs1kBlxHNKRqziWki4mJgo/", 
                                          sheet = "KoBo Onboarding(Direct)")

# Distribution Data
data_distribution_vyapaaris <- read_sheet("https://docs.google.com/spreadsheets/d/1irc7ENnrQ7w1IZ4vcBU9pzs1kBlxHNKRqziWki4mJgo/", 
                                          sheet = "Master Sheet")

# Cleaning Data 
# -------------

# Normalize field names
data_market_vyapaaris_clean <- clean_names(data_market_vyapaaris)
data_market_retailers_clean <- clean_names(data_market_retailers)
data_pitching_vyapaaris_clean <- clean_names(data_pitching_vyapaaris)
data_onboarding_vyapaaris_clean <- clean_names(data_onboarding_vyapaaris)
data_pitching_onboarding_vyapaaris_clean <- clean_names(data_pitching_onboarding_vyapaaris)
data_verification_vyapaaris_clean <- clean_names(data_verification_vyapaaris)
data_distribution_vyapaaris_clean <- clean_names(data_distribution_vyapaaris)

# Remove test entries
data_market_vyapaaris_clean <- data_market_vyapaaris_clean %>%
  filter(!(str_detect(str_to_upper(vyapaari_name), "TEST") | str_detect(str_to_upper(surveyor_name), "TEST")))

data_market_retailers_clean <- data_market_retailers_clean %>%
  filter(!str_detect(str_to_upper(saathi_name), "TEST"))

data_pitching_vyapaaris_clean <- data_pitching_vyapaaris_clean %>%
  filter(!(str_detect(str_to_upper(vyapaari_name), "TEST") | str_detect(str_to_upper(surveyor_name), "TEST")))

data_onboarding_vyapaaris_clean <- data_onboarding_vyapaaris_clean %>%
  filter(!(str_detect(str_to_upper(vyapaari_name), "TEST") | str_detect(str_to_upper(surveyor_name), "TEST")))

data_pitching_onboarding_vyapaaris_clean <- data_pitching_onboarding_vyapaaris_clean %>%
  filter(!(str_detect(str_to_upper(vyapaari_name), "TEST") | str_detect(str_to_upper(surveyor_name), "TEST")))

# Transforming Data 
# -------------

# Append the two pitching datasets
data_pitching_vyapaaris_clean <- bind_rows(data_pitching_vyapaaris_clean, data_pitching_onboarding_vyapaaris_clean)[, 1:65]

# Select Relevant Fields
# Market Survey - Vyapaaris
data_market_vyapaaris_clean <- data_market_vyapaaris_clean %>%
  select(today, surveyor_name, surveyor_location, vyapaari_gender, vyapaari_age,
         vyapaari_ward_area, vyapaari_shop_type, 
         vyapaari_iron_box_type, vyapaari_iron_box_type_before,	vyapaari_daily_clothes, vyapaari_ironing_rates, 
         vyapaari_monthly_days, vyapaari_coal_expenses_daily, vyapaari_coal_quantity,	vyapaari_coal_heating_time, 
         vyapaari_lpg_interest, vyapaari_lpg_yes, vyapaari_lpg_no, vyapaari_lpg_benefits,	vyapaari_lpg_retailer_knowledg,
         vyapaari_bank_account, vyapaari_digital_payments, vyapaari_financial_assistance, vyapaari_social_preference, 
         vyapaari_lang_read, vyapaari_lang_listen, vyapaari_association, vyapaari_lpg_prevention, 
         x_id, x_uuid, x_submission_time, x_validation_status, x_index
  ) %>%
  filter(surveyor_location == "chennai")

# Market Survey - Retailers
data_market_retailers_clean <- data_market_retailers_clean %>%
  select(today, surveyor_name = saathi_name, surveyor_location = survey_location, 
         retailer_shop_name, retailer_shop_address, retailer_shop_area, 
         retailer_primary_business, retailer_business_years, retailer_iron_box_models, retailer_iron_box_sales,
         retailer_customer_region, retailer_sales_strategy, retailer_exchange_option, retailer_iron_box_manufacturer,
         retailer_iron_box_margin, retailer_iron_box_inventory, retailer_market_estimate, 
         retailer_lpg_awareness, retailer_lpg_sales, retailer_lpg_sales_qty, retailer_lpg_sales_response, 
         retailer_lpg_sales_interest, retailer_lpg_rejection, retailer_additional_comments,
         x_id, x_uuid, x_submission_time, x_validation_status, x_index
  ) %>%
  filter(surveyor_location == "chennai")

# Pitching - Vyapaaris
data_pitching_vyapaaris_clean <- data_pitching_vyapaaris_clean %>%
  select(today, surveyor_name, surveyor_location, vyapaari_name, vyapaari_phone_number,vyapaari_gender, vyapaari_age, 
         vyapaari_address, vyapaari_ward_area, 
         vyapaari_shop_type, vyapaari_business_years, vyapaari_iron_box_type, vyapaari_iron_box_age, vyapaari_box_retailer,
         vyapaari_lpg_awareness, vyapaari_lpg_awareness_medium, vyapaari_lpg_benefits, vyapaari_lpg_interest,	vyapaari_subsidy_interest, 
         vyapaari_subsidy_interest_001, vyapaari_purchase_follow_up, vyapaari_association, saathi_onboard_debrief, 	
         x_id, x_uuid, x_submission_time, x_validation_status, x_index
  )

# Onboarding - Vyapaaris
data_onboarding_vyapaaris_clean <- data_onboarding_vyapaaris_clean %>%
  select(today, surveyor_name, surveyor_location, vyapaari_name, vyapaari_phone_number, vyapaari_payment_method, 
         vyapaari_bank_account, vyapaari_digital_payments, vyapaari_id_proofs = the_vyapaari_has_which_of_the, 
         vyapaari_ration_card_type, vyapaari_recommended, vyapaari_daily_clothes, vyapaari_ironing_rates, vyapaari_monthly_days, 
         vyapaari_shop_members, vyapaari_monthly_income, vyapaari_daily_income, vyapaari_coal_expenses, vyapaari_coal_expenses_daily,
         vyapaari_coal_quantity, vyapaari_coal_heating_time = how_much_time_in_mi_eating_up_coal_daily, vyapaari_referal,
         x_id, x_uuid, x_submission_time, x_validation_status, x_index
  )

# Vyapaari IDs Dataset
data_vyapaari_id <- data_verification_vyapaaris_clean %>%
  mutate(vyapaari_id = str_remove(vyapaari_id, " ")) %>%
  select(vyapaari_phone_number, vyapaari_id)

# Verification - Vyapaaris
data_verification_vyapaaris_clean <- data_verification_vyapaaris_clean %>%
  mutate(vyapaari_id = str_remove(vyapaari_id, " ")) %>%
  select(vyapaari_id, document_verification_status, 
         approval_for_subsidy, verified_and_approved_by, reason_for_pending_status)

# Distribution - Vyapaaris
data_distribution_vyapaaris_clean <- data_distribution_vyapaaris_clean %>%
  mutate(vyapaari_id = str_remove(vyapaari_id, " "),
         cohort = str_to_lower(str_remove(cohort, " ")),
         delivery_date = ymd(delivery_date)) %>%
  select(vyapaari_id, funder, saathi_allocation, distributor_allocation, 
         cohort, delivery_date, delivery_status_of_lpg_iron_box)

# Remove Personally Identifiable Information (PII)
## Pitching data
data_pitching_vyapaaris_clean <- data_pitching_vyapaaris_clean %>%
  inner_join(data_vyapaari_id, by = "vyapaari_phone_number") %>%
  select(-vyapaari_phone_number)

## Onboarding data
data_onboarding_vyapaaris_clean <- data_onboarding_vyapaaris_clean %>%
  inner_join(data_vyapaari_id, by = "vyapaari_phone_number") %>%
  select(-vyapaari_phone_number)

# Export data files
write_csv(data_market_vyapaaris_clean, "data/processed/eval/data_market_vyapaaris_clean.csv")
write_csv(data_market_retailers_clean, "data/processed/eval/data_market_retailers_clean.csv")
write_csv(data_pitching_vyapaaris_clean, "data/processed/eval/data_pitching_vyapaaris_clean.csv")
write_csv(data_onboarding_vyapaaris_clean, "data/processed/eval/data_onboarding_vyapaaris_clean.csv")
write_csv(data_verification_vyapaaris_clean, "data/processed/eval/data_verification_vyapaaris_clean.csv")
write_csv(data_distribution_vyapaaris_clean, "data/processed/eval/data_distribution_vyapaaris_clean.csv")
