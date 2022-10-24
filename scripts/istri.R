# Setup Work Environment
# -------------

# Clean the worskspace
rm(list = ls())

# Loading Packages
library(KoboconnectR)
library(tidyverse)
library(janitor)
library(googlesheets4)

# Identify Data
# -------------

# Load the KoboToolbox credentials
USER_ID <- Sys.getenv(c("USER_ID"))
PASSWORD <- Sys.getenv(c("PASSWORD"))

# Login into Kobo
kobo_token <- get_kobo_token(url = "kf.kobotoolbox.org", uname = USER_ID, pwd = PASSWORD)

# List of Kobo forms data to download
kobo_forms_list <- kobotools_api(url = "kf.kobotoolbox.org", simplified = T, uname = USER_ID, pwd = PASSWORD)

# Select forms to download
id_market_vyapaaris <- kobo_forms_list[kobo_forms_list$name == "Market Entry Survey and Baseline - Vyapaaris", ]$asset
id_market_retailers <- kobo_forms_list[kobo_forms_list$name == "Market Entry Survey and Baseline - Retailers", ]$asset
id_pitching_vyapaaris <- kobo_forms_list[kobo_forms_list$name == "Pitching - Vyapaaris", ]$asset
id_onbaording_vyapaaris <- kobo_forms_list[kobo_forms_list$name == "Onboarding - Vyapaaris", ]$asset
id_pitching_onbaording_vyapaaris <- kobo_forms_list[kobo_forms_list$name == "Pitching and Onboarding - Vyapaaris", ]$asset

# Downloading Data
# -------------

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
data_market_vyapaaris <- clean_names(data_market_vyapaaris)
data_market_retailers <- clean_names(data_market_retailers)
data_pitching_vyapaaris <- clean_names(data_pitching_vyapaaris)
data_onboarding_vyapaaris <- clean_names(data_onboarding_vyapaaris)
data_pitching_onboarding_vyapaaris <- clean_names(data_pitching_onboarding_vyapaaris)
data_verification_vyapaaris <- clean_names(data_verification_vyapaaris)
data_distribution_vyapaaris <- clean_names(data_distribution_vyapaaris)

# Remove test entries
data_market_vyapaaris_clean <- data_market_vyapaaris %>%
  filter(!(str_detect(str_to_upper(vyapaari_name), "TEST") | str_detect(str_to_upper(surveyor_name), "TEST")))

data_market_retailers_clean <- data_market_retailers %>%
  filter(!str_detect(str_to_upper(saathi_name), "TEST"))

data_pitching_vyapaaris_clean <- data_pitching_vyapaaris %>%
  filter(!(str_detect(str_to_upper(vyapaari_name), "TEST") | str_detect(str_to_upper(surveyor_name), "TEST")))

data_onboarding_vyapaaris_clean <- data_onboarding_vyapaaris %>%
  filter(!(str_detect(str_to_upper(vyapaari_name), "TEST") | str_detect(str_to_upper(surveyor_name), "TEST")))

data_pitching_onboarding_vyapaaris_clean <- data_pitching_onboarding_vyapaaris %>%
  filter(!(str_detect(str_to_upper(vyapaari_name), "TEST") | str_detect(str_to_upper(surveyor_name), "TEST")))

# Select Relevant Data
# Append the two pitching datasets
data_pitching_vyapaaris_clean <- bind_rows(data_pitching_vyapaaris_clean, data_pitching_onboarding_vyapaaris_clean)[, 1:64]

# Filter out verification status
data_verification_vyapaaris_clean <- data_verification_vyapaaris %>%
  mutate(vyapaari_id = str_remove(vyapaari_id, " ")) %>%
  select(vyapaari_phone_number, vyapaari_id, document_verification_status, 
         approval_for_subsidy, verified_and_approved_by, reason_for_pending_status)

# Filter out distribution status
data_distribution_vyapaaris_clean <- data_distribution_vyapaaris %>%
  mutate(vyapaari_id = str_remove(vyapaari_id, " "),
         vyapaari_phone_number = as.numeric(as.character(vyapaari_phone_number)),
         cohort = str_to_lower(str_remove(cohort, " "))) %>%
  select(vyapaari_phone_number, vyapaari_id, funder, saathi_allocation,
         distributor_allocation, cohort, delivery_date, delivery_status_of_lpg_iron_box)

# Transforming Data
# -------------

# Market Survey
# Iron box type
data_market_vyapaaris_clean <- data_market_vyapaaris_clean %>%
  mutate(vyapaari_iron_box_type_final = if_else(vyapaari_iron_box_type_lpg == 1, "lpg", 
                                                if_else(vyapaari_iron_box_type_coal == 1, "coal", "electric")))

# Joining Pitching and Onboarding Data
onboarded_vyapaaris <- data_pitching_vyapaaris_clean %>% 
  full_join(data_onboarding_vyapaaris_clean, 
            by = "vyapaari_phone_number", 
            suffix = c("_pitch", "_onboard"))

# Joining Verification and Distribution Data
implementation_master <- onboarded_vyapaaris %>%
  left_join(data_verification_vyapaaris_clean, by = "vyapaari_phone_number") %>%
  left_join(data_distribution_vyapaaris_clean, by = "vyapaari_phone_number")

# Label Vyapaari Status
implementation_master_status <- implementation_master %>%
  mutate(surveyor_name_pitch = toupper(surveyor_name_pitch),
         surveyor_name_onboard = toupper(surveyor_name_onboard),
         vyapaari_pitched = if_else(!is.na(today_pitch), "YES", "NO"),
         vyapaari_onboarded = if_else(!is.na(today_onboard), "YES", "NO"),
         vyapaari_verified = if_else(!is.na(approval_for_subsidy) & 
                                       approval_for_subsidy == "Approved", "YES", "NO"),
         vyapaari_distributed = if_else(!is.na(delivery_status_of_lpg_iron_box) & 
                                          delivery_status_of_lpg_iron_box == "Delivered", "YES", "NO")) %>%
  mutate(vyapaari_status = if_else(vyapaari_pitched == "NO", "not-pitched", 
                                   if_else(vyapaari_distributed == "YES", "distributed", 
                                           if_else(vyapaari_verified == "YES", "verified",  
                                                   if_else(vyapaari_onboarded == "YES", "onboarded",
                                                           if_else(vyapaari_pitched == "YES", "pitched",
                                                                   "other"))))))

## Clean Saathi names
saathi_names_clean <- openxlsx::read.xlsx("data/raw/saathi_names.xlsx")

implementation_master_status <- implementation_master_status %>%
  left_join(saathi_names_clean, by = c("surveyor_name_pitch" = "surveyor_name")) %>%
  left_join(saathi_names_clean, by = c("surveyor_name_onboard" = "surveyor_name")) %>%
  select(-surveyor_name_pitch, -surveyor_name_onboard) %>% 
  rename(surveyor_name_pitch = surveyor_name_clean.x) %>%
  rename(surveyor_name_onboard = surveyor_name_clean.y)

## Clean Onboarding Dates
implementation_master_status <- implementation_master_status %>%
  mutate(today_onboard = if_else(is.na(today_onboard), "", today_onboard))

## Export the data files
## Market Survey
write_csv(data_market_vyapaaris_clean, "data/processed/data_market_vyapaaris.csv")
write_csv(data_market_retailers_clean, "data/processed/data_market_retailers.csv")

## Implementationx
write_csv(implementation_master_status, "data/processed/data_implemented_vyapaaris.csv")

implementation_master_status %>%
  group_by(vyapaari_status) %>%
  summarise(count = n())
