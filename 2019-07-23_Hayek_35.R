# #!/usr/bin/env Rscript -- requires Postgres DB authentication

# 2019-07-23_Hayek_35.R

# Notes from 2019-10-15 mtg w/ Dr. Hayek:
# - Add `afffamm` (from Form A3)
# - Add MRNs `mrn` (from Form A1)
# - Add all subsequent visits of those with any blood draw
# - Add Form B4
# - Add Form D2

# *****************
# Globals & Helpers

source("~/Box/Documents/R_helpers/config.R")
source("~/Box/Documents/R_helpers/helpers.R")


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
    # , "exam_date"
    , "reg_num"
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
    fields = fields_ms #,
    # filterLogic = paste0("(",
    #                      "[subject_id] >= 'UM00000543'",
    #                      " AND ",
    #                      "[subject_id] < 'UM00002000'",
    #                      " AND ",
    #                      "[exam_date] >= '2017-03-15'",
    #                      ")")
    )

# Build df/tibble from JSON object
df_ms <- 
  json_ms %>% 
  jsonlite::fromJSON() %>% 
  as_tibble %>% 
  na_if("") %>% 
  mutate_at(vars(subject_id, reg_num), as.character) %>% 
  mutate_at(vars(race_value, sex_value, ed_level), as.integer) %>% 
  mutate(reg_num = pad_with(reg_num, "0", 9, "left"))


# ******************************
# Get UMMAP General Data ----

# Define MiNDSet fields
fields_ug_raw <-
  c(
    "subject_id"
    , "exam_date"
    , "reg_num"
  )

fields_ug <- fields_ug_raw %>% paste(collapse = ",")

# Retrieve JSON object via REDCap API
json_ug <-
  export_redcap_records(
    uri    = REDCAP_API_URI,
    token  = REDCAP_API_TOKEN_UMMAP_GEN,
    fields = fields_ug,
    filterLogic = paste0("(",
                         "[subject_id] >= 'UM00000543'",
                         " AND ",
                         # "[subject_id] < 'UM00002000'",
                         # " AND ",
                         "[exam_date] >= '2017-03-15'",
                         ")")
  )

# Build df/tibble from JSON object
df_ug <- 
  json_ug %>% 
  jsonlite::fromJSON() %>% 
  as_tibble %>% 
  select(-redcap_event_name) %>%
  na_if("") %>% 
  mutate_at(vars(subject_id, reg_num), as.character) %>% 
  mutate_at(vars(exam_date), as.Date) %>% 
  mutate(reg_num = pad_with(reg_num, "0", 9, "left"))


# Define MiNDSet blood fields
fields_ugbld_raw <-
  c(
    "subject_id"
    , "blood_drawn"
    , "blood_draw_date"
  )
fields_ugbld <- fields_ugbld_raw %>% paste(collapse = ",")

json_ugbld <-
  export_redcap_records(
    uri    = REDCAP_API_URI,
    token  = REDCAP_API_TOKEN_UMMAP_GEN,
    fields = fields_ugbld,
    filterLogic = paste0("(",
                         "[subject_id] >= 'UM00000543'",
                         # " AND ",
                         # "[subject_id] < 'UM00002000'",
                         " AND ",
                         "[blood_drawn] = 1",
                         " AND ",
                         "[blood_draw_date] != ''",
                         ")"))

df_ugbld <-
  json_ugbld %>% 
  jsonlite::fromJSON() %>% 
  as_tibble() %>% 
  select(-redcap_event_name) %>%
  na_if("") %>% 
  mutate_at(vars(subject_id), as.character) %>% 
  mutate_at(vars(blood_drawn), as.integer) %>% 
  mutate_at(vars(blood_draw_date), as.Date)


df_ugms <-
  df_ug %>% 
  left_join(df_ms,by = c("subject_id" = "subject_id",
                         "reg_num" = "reg_num"))

df_check_subject_id_reg_num <- 
  df_ugms %>% 
  select(subject_id, reg_num) %>% 
  distinct(subject_id, reg_num) %>% 
  group_by(subject_id) %>% 
  summarize(n = n()) %>% 
  ungroup()

bld_np_diff <- 183L # Dr. Hayek said within the year
df_ugbld_ugms <- 
  inner(df_ugbld, df_ugms,
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

# Add post-blooddraw subsequent visits
df_ugms_names <- names(df_ugms)
df_ugms_temp <- as_tibble(matrix(data = NA, nrow = 0, ncol = ncol(df_ugms),
                                 dimnames = list(NULL, df_ugms_names)))

for (i in seq_len(nrow(df_ugms))) {
  # cat(i)
  for (j in seq_len(nrow(df_ugbld_ugms))) {
    # cat(paste(i, j))
    if (df_ug[[i, "subject_id"]] == df_ugbld_ugms[[j, "subject_id"]] &
        df_ug[[i, "exam_date"]] > df_ugbld_ugms[[j, "exam_date"]]) {
      df_ugms_temp <- bind_rows(df_ugms_temp, df_ugms[i, ])
    }
  }
}

df_ugbld_ugms_rb <-
  bind_rows(df_ugbld_ugms, df_ugms_temp) %>% 
  arrange(subject_id, exam_date)
  

df_ugbld_ugms_rb_mut <- df_ugbld_ugms_rb %>% 
  propagate_value(id_field = subject_id, 
                  date_field = exam_date, 
                  value_field = blood_draw_date) %>% 
  propagate_value(id_field = subject_id, 
                  date_field = exam_date, 
                  value_field = blood_drawn) %>%
  mutate(blood_day_diff = 
           abs(as.integer(blood_draw_date) - as.integer(exam_date))) %>% 
  filter(!is.na(exam_date))



con <- dbConnect(RPostgres::Postgres(),
                 service = "madcbrain pgsql madc_integ", # ~/.pg_service.conf
                 user     = rstudioapi::askForPassword("PostgreSQL Username:"),
                 password = rstudioapi::askForPassword("PostgreSQL Password:"))

# dbListTables(con)

# Get `a1` data (mrn)
res_a1 <- dbSendQuery(con, read_file("sql/a1.sql"))
df_a1  <- dbFetch(res_a1)
dbClearResult(res_a1); rm(res_a1)

# Get `a3` data (afffamm)
res_a3 <- dbSendQuery(con, read_file("sql/a3.sql"))
df_a3  <- dbFetch(res_a3)
dbClearResult(res_a3); rm(res_a3)

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

# Get `b4` data
res_b4 <- dbSendQuery(con, read_file("sql/b4.sql"))
df_b4  <- dbFetch(res_b4)
dbClearResult(res_b4); rm(res_b4)

# Get `c2` data
res_c1c2 <- dbSendQuery(con, read_file("sql/c1c2.sql"))
df_c1c2  <- dbFetch(res_c1c2)
dbClearResult(res_c1c2); rm(res_c1c2)

# Get `d1` data
res_d1 <- dbSendQuery(con, read_file("sql/d1.sql"))
df_d1  <- dbFetch(res_d1)
dbClearResult(res_d1); rm(res_d1)

# Get `d2` data
res_d2 <- dbSendQuery(con, read_file("sql/d2.sql"))
df_d2  <- dbFetch(res_d2)
dbClearResult(res_d2); rm(res_d2)

# Process `dob` from `header_a1` form to age
df_a1_dob <- df_a1 %>% 
  calculate_age(dob, form_date) %>% 
  select(-dob)

# Process `d1` data down to `madc_dx`
df_d1_madc_dx <-
  df_d1 %>% 
  # coalesce IVP / FVP / TVP columns
  coalesce_ift_cols() %>% 
  # create MADC diagnosis field
  rowwise() %>% 
  mutate(madc_dx = case_when(
    sum(amndem, pca, ppasyn, ftdsyn, lbdsyn, namndem, na.rm = TRUE) > 1 ~
      "Mixed dementia",
    normcog == 1 ~ "NL",
    normcog == 0 & demented == 0 & 
      (mciamem == 1 | mciaplus == 1 | mcinon1 == 1 | mcinon2 == 1) ~ "MCI",
    normcog == 0 & demented == 0 &
      impnomci == 1 ~ "Impaired not MCI",
    normcog == 0 & demented == 1 & (amndem == 1 | namndem == 1) &
      alzdis == 1 & alzdisif == 1 ~ "AD",
    normcog == 0 & demented == 1 & lbdsyn == 1 ~ "LBD",
    normcog == 0 & demented == 1 & 
      (ftdsyn == 1 | 
         (psp == 1 & pspif == 1) |
         (cort == 1 & cortif == 1) |
         (ftldmo == 1 & ftldmoif == 1) | 
         (ftldnos == 1 & ftldnoif == 1)) ~ "FTD",
    normcog == 0 & demented == 1 &
      cvd == 1 & cvdif == 1 ~ "Vascular dementia",
    normcog == 0 & demented == 1 &
      ppasyn == 1 &
      (is.na(psp) |
         is.na(cort) |
         is.na(ftldmo) |
         is.na(ftldnos)) ~ "PPA",
    demented == 1 ~ "Other dementia",
    TRUE ~ NA_character_
  )) %>% 
  ungroup() %>% 
  select(ptid, form_date, madc_dx)


# Left Join all data by `ptid`, `form_date`

df_ugms_um <-
  df_ugbld_ugms_rb_mut %>% 
  rename(ptid = subject_id, form_date = exam_date) %>% 
  left_join(df_a1_dob,     by = c("ptid", "form_date")) %>% 
  left_join(df_d1_madc_dx, by = c("ptid", "form_date")) %>% 
  left_join(df_a3,         by = c("ptid", "form_date")) %>% 
  left_join(df_a4,         by = c("ptid", "form_date")) %>% 
  left_join(df_a5,         by = c("ptid", "form_date")) %>% 
  left_join(df_b1,         by = c("ptid", "form_date")) %>% 
  left_join(df_b4,         by = c("ptid", "form_date")) %>% 
  left_join(df_c1c2,       by = c("ptid", "form_date")) %>% 
  left_join(df_d2,         by = c("ptid", "form_date")) %>% 
  coalesce_ift_cols() %>% 
  filter(!is.na(madc_dx)) %>% 
  select(-ends_with("_complete"))

# # Force coalesce inconsistently named initial-visit / follow-up-visit fields
# df_ms_um <-
#   df_ms_um %>% 
#   coalesce()

# include MADC Consensus Diagnosis
# send data dicationary (UMMAP - UDS 3)

write_csv(df_ugms_um, 
          paste0("2019-07-23_Hayek_34_", Sys.Date(), ".csv"),
          na = "")

dbDisconnect(con); rm(con)


# Build Data Dictionary -- MiNDSet Registry ----

fields_dd_ms_um <- names(df_ugms_um)
json_dd_ms <- RCurl::postForm(
  uri          = REDCAP_API_URI,
  token        = REDCAP_API_TOKEN_MINDSET,
  content      = 'metadata',
  format       = 'json',
  returnFormat = 'json'
)
df_dd_ms <-
  json_dd_ms %>% 
  jsonlite::fromJSON() %>% 
  as_tibble() %>% 
  filter(field_name %in% fields_dd_ms_um)

write_csv(df_dd_ms, "DataDictionary_MiNDSetRegistry.csv", na = "")

# Build Data Dictionary -- UMMAP General ----

fields_dd_ug_um <- names(df_ugms_um)
json_dd_ug <- RCurl::postForm(
  uri          = REDCAP_API_URI,
  token        = REDCAP_API_TOKEN_UMMAP_GEN,
  content      = 'metadata',
  format       = 'json',
  returnFormat = 'json'
)
df_dd_ug <-
  json_dd_ug %>% 
  jsonlite::fromJSON() %>% 
  as_tibble() %>% 
  filter(field_name %in% fields_dd_ug_um)

write_csv(df_dd_ug, "DataDictionary_UMMAP-General.csv", na = "")

# Build Data Dictionary -- UMMAP - UDS 3 ----

fields_dd_u3_um <- names(df_ugms_um)
json_dd_u3 <- RCurl::postForm(
  uri          = REDCAP_API_URI,
  token        = REDCAP_API_TOKEN_UDS3n,
  content      = 'metadata',
  format       = 'json',
  returnFormat = 'json'
)
df_dd_u3 <-
  json_dd_u3 %>% 
  jsonlite::fromJSON() %>% 
  as_tibble() %>% 
  filter(field_name %in% fields_dd_u3_um)

write_csv(df_dd_u3, "DataDictionary_UMMAP-UDS3.csv", na = "")



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
