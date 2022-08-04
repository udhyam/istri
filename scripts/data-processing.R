# Clean the workspace
rm(list=ls())

## Load the required libraries
library(readxl)
library(janitor)
library(tidyverse)
library(lubridate)

## Market Evaluation
## Read the datasets
market_eval_vyapaaris <- read_excel("data/raw/001-market-eval.xlsx")
market_eval_retailers <- read_excel("data/raw/001-market-eval-retailers.xlsx")

## Clean the field names
market_eval_vyapaaris <- clean_names(market_eval_vyapaaris)
market_eval_retailers <- clean_names(market_eval_retailers)

## Clean the datasets

## Vyapaaris
market_eval_vyapaaris_clean <- market_eval_vyapaaris %>%
  # Normalise iron box type
  mutate(vyapaari_iron_box_type_clean = if_else(vyapaari_iron_box_type == "coal electric", "coal", 
                                        if_else(vyapaari_iron_box_type == "coal lpg", "lpg",
                                        if_else(vyapaari_iron_box_type == "lpg electric", "lpg",
                                                vyapaari_iron_box_type)))) %>%
  # Update daily and monthly income
  mutate(vyapaari_monthly_income_clean = if_else(!is.na(vyapaari_monthly_income), 
                                                 vyapaari_monthly_income, (vyapaari_daily_income*vyapaari_monthly_days))) %>%
  mutate(vyapaari_daily_income_clean = if_else(!is.na(vyapaari_daily_income), 
                                               vyapaari_daily_income, (vyapaari_monthly_income/vyapaari_monthly_days))) %>%
  # Update daily and monthly coal expenses
  mutate(vyapaari_coal_expenses_monthly_clean = if_else(!is.na(vyapaari_coal_expenses), 
                                                        vyapaari_coal_expenses, (vyapaari_coal_expenses_daily*vyapaari_monthly_days))) %>%
  mutate(vyapaari_coal_expenses_daily_clean = if_else(!is.na(vyapaari_coal_expenses_daily), 
                                                      vyapaari_coal_expenses_daily, (vyapaari_coal_expenses/vyapaari_monthly_days))) %>%
  # Add date field
  mutate(today_date = ymd(today))

## Retailers
market_eval_retailers_clean <- market_eval_retailers


## Pending
## - Club locations by zones
## - Age range

# Review the datasets
glimpse(market_eval_vyapaaris_clean)
glimpse(market_eval_retailers_clean)

# Export CSV files
write_csv(market_eval_vyapaaris_clean, "data/processed/market_eval_vyapaaris.csv")
write_csv(market_eval_retailers_clean, "data/processed/market_eval_retailers.csv")