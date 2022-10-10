# pipeline.R --------------------------------------------------------------
# This script is used to run the RAP for the dfm annual publication

# 1. install required packages --------------------------------------------
# TODO: investigate using renv package for dependency management
req_pkgs <- c("dplyr", "stringr", "data.table", "yaml", "openxlsx","rmarkdown",
              "logr", "highcharter", "lubridate", "dbplyr","tidyr","janitor", "DBI")


#uncomment if package installs are needed
 utils::install.packages(req_pkgs, dependencies = TRUE)

devtools::install_github(
  "nhsbsa-data-analytics/gphsR",
  auth_token = Sys.getenv("GITHUB_PAT")
)

 devtools::install_github("nhsbsa-data-analytics/nhsbsaR")
#add dfmR here when functions complete
invisible(lapply(c(req_pkgs,  "nhsbsaR", "gphsR"), library, character.only = TRUE))

# 2. setup logging --------------------------------------------------------

#lf <- logr::log_open(autolog = TRUE)

# send code to log
#logr::log_code()

# 3. set options ----------------------------------------------------------

gphsR::gphs_options()
hcoptslang <- getOption("highcharter.lang")
hcoptslang$thousandsSep <- ","
# 4. extract data from NHSBSA DWH -----------------------------------------
# build connection to database
con <- con_nhsbsa(
  dsn = "FBS_8192k",
  driver = "Oracle in OraClient19Home1",
  "DWCP",
  username = rstudioapi::showPrompt(title = "Username", message = "Username"),
  password = rstudioapi::askForPassword()
)
#run functions for data and write to csv (remove csv when table bult complete)

national_extract <- national_extract(
con = con,
table = "GPS_FINAL_DIM"
)
write.csv(national_extract,"national_extract.csv")
national_month_extract <- national_month_extract(
  con = con,
  table = "GPS_MONTH_DIM"
)
write.csv(national_month_extract,"national_month_extract.csv")
icb_extract <- icb_extract(
  con = con,
  table = "GPS_FINAL_DIM"
)
write.csv(icb_extract,"icb_extract.csv")


# create ref for flu
ref_data <- list()

# import 2015/16 flu data not held in DWH
ref_data$flu_data_1516v1 <- dplyr::select(
  data.table::fread(list.files("./Ref/Flu",full.names = TRUE)),
  FINANCIAL_YEAR,APPLIANCE_DISPENSER_HIST,num_flu_pharm,	flu_items_total,	flu_cost_total,	flu_fees_total
)

national_extract<- national_extract %>%
  #filter(APPLIANCE_DISPENSER_HIST == "N") %>%
  # join the 15/16 flu csv data to main data on year and dispenser code
  left_join(
    ref_data$flu_data_1516,
    by = c("FINANCIAL_YEAR","APPLIANCE_DISPENSER_HIST"),
    suffix = c("_dwh","_csv")
  )
# %>%
# # compute final flu columns, replacing NAs with 0
# mutate(num_flu_pharm = case_when(FINANCIAL_YEAR == "2015/2016" ~ num_flu_pharm_csv,
#                         TRUE ~ num_flu_pharm_dwh),
#   flu_items_total = case_when(FINANCIAL_YEAR == "2015/2016" ~ flu_items_total_csv,
#                         TRUE ~ flu_items_total_dwh),
#   flu_cost_total = case_when(FINANCIAL_YEAR == "2015/2016" ~ flu_cost_total_csv,
#                        TRUE ~ flu_cost_total_dwh),
#   flu_fees_total = case_when(FINANCIAL_YEAR == "2015/2016" ~ flu_fees_total_csv,
#                        TRUE ~ flu_fees_total_dwh)
# ) %>%
# # drop excess cols
# select(-(ends_with("_csv")),-(ends_with("_dwh")))

rlang::last_error()
rlang::last_trace()

# 5. write data to .xlsx -

# 6. Automate QR table - tba

# 7. Automate Narrative - tba

# 8. render markdown ------------------------------------------------------
  rmarkdown::render("gphs_annual_narrative.Rmd",
                    output_format = "html_document",
                    output_file = "outputs/gphs_annual_2021_22_v001.html")

  rmarkdown::render("gphs_annual_narrative.Rmd",
                    output_format = "word_document",
                    output_file = "outputs/gphs_annual_2021_22_v001.docx")
