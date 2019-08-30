#!/usr/bin/env Rscript

# 2019-07-23_Hayek_35.R


# *****************
# Globals & Helpers

source("~/Box Sync/Documents/R_helpers/config.R")
source("~/Box Sync/Documents/R_helpers/helpers.R")


# *********
# Libraries

# Database
load_libraries(c("DBI"))
# Munging
load_libraries(c("dplyr", "readr", "stringr", "FuzzyDateJoin"))


# ******************************
# Get MiNDSET Registry Data ----

# Define MiNDSet fields
fields_ms_raw <-
  c(
    "subject_id"
    , "exam_date"
    , "sex_value"
    , "race_value"
    , "ed_level"
  )

fields_ms <- fields_ms_raw %>% paste(collapse = ",")

# Retrieve JSON object via REDCap API
json_ms <-
  export_redcap_records(
    uri    = REDCAP_API_URI,
    token  = REDCAP_API_TOKEN_MINDSET,
    fields = fields_ms,
    filterLogic = paste0("(",
                         "[subject_id] >= 'UM00000543'",
                         " AND ",
                         "[subject_id] < 'UM00002000'",
                         " AND ",
                         "[exam_date] >= '2017-03-15'",
                         ")"))

# Build df/tibble from JSON object
df_ms <- 
  json_ms %>% 
  jsonlite::fromJSON() %>% 
  as_tibble %>% 
  select(-redcap_event_name) %>% 
  na_if("") %>% 
  mutate_at(vars(subject_id), as.character) %>% 
  mutate_at(vars(race_value, sex_value, ed_level), as.integer) %>%
  mutate_at(vars(exam_date), as.Date)


# Define MiNDSet blood fields
fields_msbld_raw <-
  c(
    "subject_id"
    , "blood_drawn"
    , "blood_draw_date"
  )
fields_msbld <- fields_msbld_raw %>% paste(collapse = ",")

json_msbld <-
  export_redcap_records(
    uri    = REDCAP_API_URI,
    token  = REDCAP_API_TOKEN_MINDSET,
    fields = fields_msbld,
    filterLogic = paste0("(",
                         "[subject_id] >= 'UM00000543'",
                         " AND ",
                         "[subject_id] < 'UM00002000'",
                         " AND ",
                         "[blood_drawn] = 1",
                         " AND ",
                         "[blood_draw_date] != ''",
                         ")"))

df_msbld <-
  json_msbld %>% 
  jsonlite::fromJSON() %>% 
  as_tibble() %>% 
  select(-redcap_event_name) %>%
  na_if("") %>% 
  mutate_at(vars(subject_id), as.character) %>% 
  mutate_at(vars(blood_drawn), as.integer) %>% 
  mutate_at(vars(blood_draw_date), as.Date)


bld_np_diff <- 183L
df_msbld_ms <- 
  outer_left(df_msbld, df_ms,
             x_id_col = "subject_id", y_id_col = "subject_id",
             x_date_col = "blood_draw_date", y_date_col = "exam_date",
             x_intvl_less = bld_np_diff, x_intvl_more = bld_np_diff) %>% 
  as_tibble() %>% 
  select(subject_id = subject_id_x
         , blood_draw_date
         , blood_drawn
         , exam_date
         , sex_value
         , race_value
         , ed_level
         , everything()
         , -subject_id_y) 

df_msbld_ms <- df_msbld_ms %>% 
  mutate(blood_day_diff = 
           abs(as.integer(blood_draw_date) - as.integer(exam_date))) %>% 
  filter(!is.na(exam_date))



con <- dbConnect(RPostgres::Postgres(),
                 service = "madcbrain pgsql madc_integ", # ~/.pg_service.conf
                 user     = rstudioapi::askForPassword("PostgreSQL Username:"),
                 password = rstudioapi::askForPassword("PostgreSQL Password:"))

dbListTables(con)

# Get `a4` data
res_a4 <- dbSendQuery(con, read_file("sql/a4.sql"))
df_a4  <- dbFetch(res_a4)
dbClearResult(res_a4); rm(res_a4)

# Get `a5` data
res_a5 <- dbSendQuery(con, read_file("sql/a5.sql"))
df_a5  <- dbFetch(res_a5)
dbClearResult(res_a5); rm(res_a5)

# Get `b1` data
res_b1 <- dbSendQuery(con, read_file("sql/b1.sql"))
df_b1  <- dbFetch(res_b1)
dbClearResult(res_b1); rm(res_b1)

# Get `c2` data
res_c1 <- dbSendQuery(con, read_file("sql/c1.sql"))
df_c1  <- dbFetch(res_c1)
dbClearResult(res_c1); rm(res_c1)


# Left Join all data by `ptid`, `form_date`

df_ms_um <-
  df_msbld_ms %>% 
  rename(ptid = subject_id, form_date = exam_date) %>% 
  left_join(df_a4, by = c("ptid", "form_date")) %>% 
  left_join(df_a5, by = c("ptid", "form_date")) %>% 
  left_join(df_b1, by = c("ptid", "form_date")) %>% 
  left_join(df_c1, by = c("ptid", "form_date")) %>% 
  coalesce_ift_cols() %>% 
  select(-ends_with("_complete"))


write_csv(df_ms_um, 
          paste0("2019-07-23_Hayek_34_", Sys.Date(), ".csv"),
          na = "")



###@    #==--  :  --==#    @##==---==##@##==---==##@    #==--  :  --==#    @###
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
#  @##==---==##@##==---==##@    EXTRA  :  SPACE    @##==---==##@##==---==##@  #
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
###@    #==--  :  --==#    @##==---==##@##==---==##@    #==--  :  --==#    @###
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
#  @##==---==##@##==---==##@    EXTRA  :  SPACE    @##==---==##@##==---==##@  #
#@##==---==##@   @##==---==##@    #==-- --==#    @##==---==##@   @##==---==##@#
##==---==##@   #   @##==---==##@    #==-==#    @##==---==##@   #   @##==---==##
#=---==##@    #=#    @##==---==##@    #=#    @##==---==##@    #=#    @##==---=#
#--==##@    #==-==#    @##==---==##@   #   @##==---==##@    #==-==#    @##==--#
#==##@    #==-- --==#    @##==---==##@   @##==---==##@    #==-- --==#    @##==#
###@    #==--  :  --==#    @##==---==##@##==---==##@    #==--  :  --==#    @###
