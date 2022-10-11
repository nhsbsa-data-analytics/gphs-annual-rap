# pipeline.R --------------------------------------------------------------
# This script is used to run the RAP for the dfm annual publication

# 1. install required packages --------------------------------------------
# TODO: investigate using renv package for dependency management
req_pkgs <- c("dplyr", "stringr", "data.table", "yaml", "openxlsx","rmarkdown",
              "logr", "highcharter", "lubridate", "dbplyr","tidyr","janitor", "DBI")


#uncomment if package installs are needed
# utils::install.packages(req_pkgs, dependencies = TRUE)

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
ref_data$flu_data_1516 <- dplyr::select(
  data.table::fread(list.files("./Ref/Flu",full.names = TRUE)),
  FINANCIAL_YEAR,	,num_flu_pharm,	flu_items_total,	flu_cost_total,	flu_fees_total
)

flu_data<- national_extract %>%
  filter(APPLIANCE_DISPENSER_HIST == "N") %>%
  # join the 15/16 flu csv data to main data on year and dispenser code
  left_join(
    ref_data$flu_data_1516,
    by = c("FINANCIAL_YEAR"),
    suffix = c("_dwh","_csv")
  )
#%>%
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

#rlang::last_error()
#rlang::last_trace()
# 5. write data to .xlsx -


# create cost and items workbook
# create wb object
# create list of sheetnames needed (overview and metadata created automatically)

# create wb patient demographics object
# create list of sheetnames needed (overview and metadata created automatically)

workbook_data <- list()

workbook_data$patient_id <- capture_rate %>%
  dplyr::select(`Financial Year`,
                `Drug Category`,
                `Identified Patient Rate (%) ` = RATE)

workbook_data$national_total <- national_extract %>%
  dplyr::select(`Financial Year`,
               `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$national_category <- category_extract %>%
  dplyr::ungroup() %>%
  dplyr::select(`Financial Year`,
                `Drug Category`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$icb_total <- icb_extract %>%
  dplyr::select(`Financial Year`,
                `Integrated Care Board Name`,
                `Integrated Care Board Code`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$icb_category <- icb_category_extract %>%
  dplyr::select(`Financial Year`,
                `Integrated Care Board Name`,
                `Integrated Care Board Code`,
                `Drug Category`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$national_population <- population_extract
workbook_data$category_population <- population_category_extract

#need to put sheetnames list and workbook creation above metadata code
sheetNames <- c("Patient_Identification",
                "Table_1",
                "Table_2",
                "Table_3",
                "Table_4",
                "Table_5",
                "Table_6")

wb <- create_wb(sheetNames)

#create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "Drug Category",
  "Financial Year",
  "Mid-Year Population Year",
  "Mid-Year England Population Estimate",
  "Patients per 1000 Population",
  "Identified Patient",
  "Integrated Care Board Code",
  "Integrated Care Board Name",
  "Total Patients",
  "Total Items",
  "Total Net Ingredient Cost (GBP)"

)

meta_descs <-
  c(
    "The category of class of drug that can cause dependence.",
    "The financial year to which the data belongs.",
    "The year in which population estimates were taken, required due to the presentation of this data in financial year format.",
    "The population estimate for the corresponding Mid-Year Population Year.",
    "(Total Identified Patients / Mid-Year England Population Estimate) * 1000.",
    "This shows where an item has been attributed to an NHS number that has been verified by the Personal Demographics Service (PDS).",
    "The unique code used to refer to an Integrated Care Board (ICB).",
    "The name given to the Integrated Care Board (ICB) that a prescribing organisation belongs to. This is based upon NHSBSA administrative records, not geographical boundaries and more closely reflect the operational organisation of practices than other geographical data sources.",
    "Where patients are identified via the flag, the number of patients that the data corresponds to. This will always be 0 where 'Identified Patient' = N.",
    "The number of prescription items dispensed. 'Items' is the number of times a product appears on a prescription form. Prescription forms include both paper prescriptions and electronic messages.",
    "Total Net Ingredient Cost is the amount that would be paid using the basic price of the prescribed drug or appliance and the quantity prescribed. Sometimes called the 'Net Ingredient Cost' (NIC). The basic price is given either in the Drug Tariff or is determined from prices published by manufacturers, wholesalers or suppliers. Basic price is set out in Parts 8 and 9 of the Drug Tariff. For any drugs or appliances not in Part 8, the price is usually taken from the manufacturer, wholesaler or supplier of the product. This is given in GBP (Â£)."
  )

create_metadata(wb,
                meta_fields,
                meta_descs
)


#### Patient identification
# write data to sheet
write_sheet(
  wb,
  "Patient_Identification",
  "Dependency Forming Medicines - 2015/16 to 2021/22 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  workbook_data$patient_id,
  42
)
#left align columns A to C
format_data(wb,
            "Patient_Identification",
            c("A", "B"),
            "left",
            "")
#right align columns and round to 2 DP - D (if using long data not pivoting wider) (!!NEED TO UPDATE AS DATA EXPANDS!!)
format_data(wb,
            "Patient_Identification",
            c("C"),
            "right",
            "0.00")

#### Total items data
# write data to sheet
#suggest note 3. could be condensed to something like "Total costs and items may not match those in our Prescription Cost Analysis (PCA) publication, as they are based on a prescribing view while PCA uses a dispensing view instead."
write_sheet(
  wb,
  "Table_1",
  "Table 1: Dependency Forming Medicines - England 2015/16 to 2021/22 Total dispensed items and costs per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. Total costs and items may not be reconciled back to Prescribing Cost Analysis (PCA) publication figures as these figures are based around a 'prescribing view' of the data. This is where we use the drug or device that was prescribed to a patient, rather than the drug that was reimbursed to the dispenser to classify a prescription item. PCA uses a dispensing view where the inverse is true."
  ),
  workbook_data$national_total,
  14
)

#left align columns A to C
format_data(wb,
            "Table_1",
            c("A", "B"),
            "left",
            "")

#right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "Table_1",
            c("C", "D"),
            "right",
            "#,##0")

#right align column F and round to 2dp with thousand separator
format_data(wb,
            "Table_1",
            c("E"),
            "right",
            "#,##0.00")

#### National Paragraph data
# write data to sheet
write_sheet(
  wb,
  "Table_2",
  "Table 2: Dependency Forming Medicines - England 2015/16 to 2021/22 Yearly totals split by Drug Category and identified patients",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."

  ),
  workbook_data$national_category,
  14
)
#left align columns A to D
format_data(wb,
            "Table_2",
            c("A", "B","C"),
            "left",
            "")

#right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "Table_2",
            c("D", "E"),
            "right",
            "#,##0")

#right align column G and round to 2dp with thousand separator
format_data(wb,
            "Table_2",
            c("F"),
            "right",
            "#,##0.00")
#### ICB data
# write data to sheet
write_sheet(
  wb,
  "Table_3",
  "Table 3: Dependency Forming Medicines - England 2015/16 to 2021/22 Yearly totals split by Integrated Care Board and identified patients",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. Integrated Care Boards (ICBs) succeeded sustainability and transformation plans (STPs) and replaced the functions of clinical commissioning groups (CCGs) in July 2022 with ICB sub locations replacing CCGs during the transition period of 2022/23."

  ),
  workbook_data$icb_total,
  14
)
#left align columns A to D
format_data(wb,
            "Table_3",
            c("A", "B","C","D"),
            "left",
            "")

#right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "Table_3",
            c("E", "F"),
            "right",
            "#,##0")

#right align column  and round to 2dp with thousand separator
format_data(wb,
            "Table_3",
            c("G"),
            "right",
            "#,##0.00")

#### ICB data
# write data to sheet
write_sheet(
  wb,
  "Table_4",
  "Table 4: Dependency Forming Medicines - England 2015/16 to 2021/22 Yearly totals split by Integrated Care Board and identified patients",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. Integrated Care Boards (ICBs) succeeded sustainability and transformation plans (STPs) and replaced the functions of clinical commissioning groups (CCGs) in July 2022 with ICB sub locations replacing CCGs during the transition period of 2022/23."

  ),
  workbook_data$icb_category,
  14
)
#left align columns A to E
format_data(wb,
            "Table_4",
            c("A", "B","C","D","E"),
            "left",
            "")

#right align columns F and G and round to whole numbers with thousand separator
format_data(wb,
            "Table_4",
            c("F", "G"),
            "right",
            "#,##0")

#right align column H and round to 2dp with thousand separator
format_data(wb,
            "Table_4",
            c("H"),
            "right",
            "#,##0.00")

#### National Population
# write data to sheet
write_sheet(
  wb,
  "Table_5",
  "Table 5: Dependency Forming Medicines - 2015/16 to 2021/22 - Population totals split by financial year",
  c(
    "1. ONS population estimates taken from https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates.",
    "2. ONS population estimates for 2021/2022 were not available prior to publication so the figures for 2021 are taken from the Census figures found here https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/bulletins/populationandhouseholdestimatesenglandandwales/census2021"

  ),
  workbook_data$national_population,
  14
)
#left align columns A to B
format_data(wb,
            "Table_5",
            c("A", "B"),
            "left",
            "")

#right align columns C and D and round to whole numbers with thousand separator
format_data(wb,
            "Table_5",
            c("C", "D"),
            "right",
            "#,##0")

#right align column E and round to 2dp with thousand separator
format_data(wb,
            "Table_5",
            c("E"),
            "right",
            "#,##0.00")
#### National Category
# write data to sheet
write_sheet(
  wb,
  "Table_6",
  "Table 6: Dependency Forming Medicines - 2015/16 to 2021/22 - Population totals split by financial year and drug category",
  c(
    "1. ONS population estimates taken from https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates.",
    "2. ONS population estimates for 2021/2022 were not available prior to publication so the figures for 2021 are taken from the Census figures found here https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/bulletins/populationandhouseholdestimatesenglandandwales/census2021"

  ),
  workbook_data$category_population,
  14
)
#left align columns A to C
format_data(wb,
            "Table_6",
            c("A", "B","C"),
            "left",
            "")

#right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "Table_6",
            c("D", "E"),
            "right",
            "#,##0")

#right align column F and round to 2dp with thousand separator
format_data(wb,
            "Table_6",
            c("F"),
            "right",
            "#,##0.00")

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/dfm_2021_2022_costs_and_items_v001.xlsx",
                       overwrite = TRUE)

rm(wb)
# create wb object
# create list of sheetnames needed (overview and metadata created automatically)

workbook_data <- list()

workbook_data$patient_id <- capture_rate %>%
  dplyr::select(`Financial Year`,
                `Drug Category`,
                `Identified Patient Rate` = RATE)

workbook_data$national_total <- national_extract %>%
  dplyr::select(`Financial Year`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)

workbook_data$national_sex <- gender_extract %>%
  dplyr::select(`Financial Year`,
                `Patient Sex`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$category_sex <- gender_category_extract %>%
  apply_sdc(level = 5, rounding = F, mask = NA_real_) %>%
  dplyr::select(`Financial Year`,
                `Drug Category`,
                `Patient Sex`,
                `Identified Patient Flag`,
                `Total Identified Patients`=`sdc_Total Identified Patients`,
                `Total Items`=`sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$national_age <- ageband_data %>%
  dplyr::select(`Financial Year`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)

workbook_data$category_age <- ageband_category_data %>%
  apply_sdc(level = 5, rounding = F, mask = NA_real_) %>%
  dplyr::select(`Financial Year`,
                `Drug Category`,
                `Age Band`,
                `Identified Patient Flag`,
                `Total Identified Patients`=`sdc_Total Identified Patients`,
                `Total Items`=`sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$national_age_sex <- age_gender_data %>%
  dplyr::filter(`Identified Patient Flag`=="Y") %>%
  dplyr::select(`Financial Year`,
                `Age Band`,
                `Patient Sex`,
                `Identified Patient Flag`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$category_age_sex <- age_gender_cat_data %>%
  dplyr::filter(`Identified Patient Flag`=="Y") %>%
  apply_sdc(level = 5, rounding = F, mask = NA_real_) %>%
  dplyr::select(`Financial Year`,
                `Drug Category`,
                `Age Band`,
                `Patient Sex`,
                `Identified Patient Flag`,
                `Total Identified Patients`=`sdc_Total Identified Patients`,
                `Total Items`=`sdc_Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$national_imd <- imd_extract %>%
  dplyr::select(`Financial Year`,
                `IMD Quintile`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$category_imd <- imd_category_extract %>%
  dplyr::select(`Financial Year`,
                `Drug Category`,
                `IMD Quintile`,
                `Total Identified Patients`,
                `Total Items`,
                `Total Net Ingredient Cost (GBP)`)
workbook_data$coprescribing <- coprescribing_extract %>%
  dplyr::select(`Year Month` ,
                `Number of Categories`,
                `Total Identified Patients`)
workbook_data$coprescribing_combination <- coprescribing_matrix_extract %>%
  dplyr::select(`Year Month` ,
                `Drug Combination` =COMBINATION,
                `Total Identified Patients`)


#need to put sheetnames list and workbook creation above metadata code
sheetNames <- c("Patient_Identification",
                "Table_1",
                "Table_2",
                "Table_3",
                "Table_4",
                "Table_5",
                "Table_6",
                "Table_7",
                "Table_8",
                "Table_9",
                "Table_10",
                "Table_11")


wb <- create_wb(sheetNames)

#create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "Financial Year",
  "Year Month",
  "Identified Patient",
  "Total Items",
  "Total Net Ingredient Cost (GBP)",
  "Total Patients",
  "Drug Category",
  "Patient Sex",
  "Age Band",
  "IMD Quintile"

)

meta_descs <-
  c(
    "The financial year to which the data belongs.",
    "The year and month to which the data belongs, denoted in YYYYMM format.",
    "This shows where an item has been attributed to an NHS number that has been verified by the Personal Demographics Service (PDS).",
    "The number of prescription items dispensed. 'Items' is the number of times a product appears on a prescription form. Prescription forms include both paper prescriptions and electronic messages.",
    "Total Net Ingredient Cost is the amount that would be paid using the basic price of the prescribed drug or appliance and the quantity prescribed. Sometimes called the 'Net Ingredient Cost' (NIC). The basic price is given either in the Drug Tariff or is determined from prices published by manufacturers, wholesalers or suppliers. Basic price is set out in Parts 8 and 9 of the Drug Tariff. For any drugs or appliances not in Part 8, the price is usually taken from the manufacturer, wholesaler or supplier of the product. This is given in GBP (Â£).",
    "Where patients are identified via the flag, the number of patients that the data corresponds to. This will always be 0 where 'Identified Patient' = N.",
    "The category of class of drug that can cause dependence.",
    "The sex of the patient as noted at the time the prescription was processed. This includes where the patient has been identified but the sex has not been recorded.",
    "The age band of the patient as of the 30th September of the corresponding financial year the drug was prescribed.",
    "The IMD Quintile of the patient, based on the location of their practice, where '1' is the 20% of areas with the highest deprivation score in the Index of Multiple Deprivation (IMD) from the English Indices of Deprivation 2019, and '5' is the 20% of areas with the lowest IMD deprivation score. Unknown values are where we have been unable to match the patient postcode to a postcode in the National Statistics Postcode Lookup (NSPL)."

  )

create_metadata(wb,
                meta_fields,
                meta_descs
)

#### Patient identification
# write data to sheet
write_sheet(
  wb,
  "Patient_Identification",
  "Dependency Forming Medicines - 2015/16 to 2021/22 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  workbook_data$patient_id,
  42
)
#left align columns A to C
format_data(wb,
            "Patient_Identification",
            c("A", "B" ),
            "left",
            "")
#right align columns and round to 2 DP - D
format_data(wb,
            "Patient_Identification",
            c("C"),
            "right",
            "0.00")

#### Total items data
# write data to sheet
write_sheet(
  wb,
  "Table_1",
  "Table 1: Dependency Forming Medicines - England 2015/16 to 2021/22 Total dispensed items and costs per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. Total costs and items may not be reconciled back to Prescribing Cost Analysis (PCA) publication figures as these figures are based around a 'prescribing view' of the data. This is where we use the drug or device that was prescribed to a patient, rather than the drug that was reimbursed to the dispenser to classify a prescription item. PCA uses a dispensing view where the inverse is true."
  ),
  workbook_data$national_total,
  14
)

#left align columns A to B
format_data(wb,
            "Table_1",
            c("A", "B"),
            "left",
            "")

#right align columns C and D and round to whole numbers with thousand separator
format_data(wb,
            "Table_1",
            c("C", "D"),
            "right",
            "#,##0")

#right align column E and round to 2dp with thousand separator
format_data(wb,
            "Table_1",
            c("E"),
            "right",
            "#,##0.00")

#### National Sex data
# write data to sheet
write_sheet(
  wb,
  "Table_2",
  "Table 2: Dependency Forming Medicines - England 2015/16 to 2021/22  National prescribing by sex per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. It is possible for a patient to be marked as 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together.",
    "3. The NHSBSA does not use the latest national data standard relating to patient gender/sex, and use historic nomenclature in some cases. Please see the detailed Background Information and Methodology notice released with this publication for further information."

  ),
  workbook_data$national_sex,
  14
)
#left align columns A to C
format_data(wb,
            "Table_2",
            c("A", "B","C"),
            "left",
            "")

#right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "Table_2",
            c("D", "E"),
            "right",
            "#,##0")

#right align column F and round to 2dp with thousand separator
format_data(wb,
            "Table_2",
            c("F"),
            "right",
            "#,##0.00")

#### Category Sex data
# write data to sheet
write_sheet(
  wb,
  "Table_3",
  "Table 3: Dependency Forming Medicines - England 2015/16 to 2021/22  Yearly patients, items and costs by Drug category split by patient sex",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. It is possible for a patient to be marked as 'unknown' or 'indeterminate'. Due to the low number of patients that these two groups contain the NHSBSA has decided to group these classifications together.",
    "3. The NHSBSA does not use the latest national data standard relating to patient gender/sex, and use historic nomenclature in some cases. Please see the detailed Background Information and Methodology notice released with this publication for further information.",
    "4. Statistical disclosure control is applied where prescribing relates to fewer than 5 patients. In these instances, figures are replaced with an asterisk (*)"

  ),
  workbook_data$category_sex,
  14
)
#left align columns A to D
format_data(wb,
            "Table_3",
            c("A", "B", "C", "D"),
            "left",
            "")

#right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "Table_3",
            c("E", "F"),
            "right",
            "#,##0")

#right align column G and round to 2dp with thousand separator
format_data(wb,
            "Table_3",
            c("G"),
            "right",
            "#,##0.00")

#### National age data
# write data to sheet
write_sheet(
  wb,
  "Table_4",
  "Table 4: Dependency Forming Medicines - England 2015/16 to 2021/22 National prescribing by age per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab."

  ),
  workbook_data$national_age,
  14
)
#left align columns A to C
format_data(wb,
            "Table_4",
            c("A", "B","C"),
            "left",
            "")

#right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "Table_4",
            c("D", "E"),
            "right",
            "#,##0")

#right align column F and round to 2dp with thousand separator
format_data(wb,
            "Table_4",
            c("F"),
            "right",
            "#,##0.00")

#### Paragraph Age data
# write data to sheet
write_sheet(
  wb,
  "Table_5",
  "Table 5: Dependency Forming Medicines - England 2015/16 to 2021/22  Yearly patients, items and costs by Drug Category split by patient sex",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear blank."

  ),
  workbook_data$category_age,
  14
)
#left align columns A to D
format_data(wb,
            "Table_5",
            c("A", "B", "C", "D"),
            "left",
            "")

#right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "Table_5",
            c("E", "F"),
            "right",
            "#,##0")

#right align column G and round to 2dp with thousand separator
format_data(wb,
            "Table_5",
            c("G"),
            "right",
            "#,##0.00")
#### National age sex data
# write data to sheet
write_sheet(
  wb,
  "Table_6",
  "Table 6: Dependency Forming Medicines - England 2015/16 to 2021/22 National prescribing by age and sex per financial year",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. These totals only include patients where both age and sex are known."

  ),
  workbook_data$national_age_sex,
  14
)
#left align columns A to D
format_data(wb,
            "Table_6",
            c("A", "B", "C", "D"),
            "left",
            "")

#right align columns E and F and round to whole numbers with thousand separator
format_data(wb,
            "Table_6",
            c("E", "F"),
            "right",
            "#,##0")

#right align column G and round to 2dp with thousand separator
format_data(wb,
            "Table_6",
            c("G"),
            "right",
            "#,##0.00")

#### Category Age Sex data
# write data to sheet
write_sheet(
  wb,
  "Table_7",
  "Table 7: Dependency Forming Medicines - England 2015/16 to 2021/22  Yearly patients, items and costs by Drug Category split by patient age and sex",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients.",
    "3. Statistical disclosure control has been applied to cells containing 5 or fewer patients or items. These cells will appear blank."
  ),
  workbook_data$category_age_sex,
  14
)
#left align columns A to E
format_data(wb,
            "Table_7",
            c("A", "B", "C", "D", "E"),
            "left",
            "")

#right align columns F and G and round to whole numbers with thousand separator
format_data(wb,
            "Table_7",
            c("F","G"),
            "right",
            "#,##0")

#right align column H and round to 2dp with thousand separator
format_data(wb,
            "Table_7",
            c("H"),
            "right",
            "#,##0.00")


#### National imd data
# write data to sheet
write_sheet(
  wb,
  "Table_8",
  "Table 8: Dependency Forming Medicines - England 2015/16 to 2021/22 Yearly patients, items and costs by IMD Quintile",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. IMD Quintiles used are taken from the English Indices of Deprivation 2019 National Statistics publication.",
    "3. Where a patients lower-layer super output areas (LSOAs) is not available or has not been able to to be matched to National Statistics Postcode Lookup (May 2022) the records are reported as 'unknown' IMD Quintile.",
    "4. The patients lower-layer super output areas (LSOAs) is taken from electronic prescribing only meaning there are a higher number of patients registered as unknown in the earlier years when electronic prescribing was less prevalent.",
    "5. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."

  ),
  workbook_data$national_imd,
  14
)
#left align columns A to B
format_data(wb,
            "Table_8",
            c("A", "B"),
            "left",
            "")

#right align columns C and D and round to whole numbers with thousand separator
format_data(wb,
            "Table_8",
            c("C", "D"),
            "right",
            "#,##0")

#right align column E and round to 2dp with thousand separator
format_data(wb,
            "Table_8",
            c("E"),
            "right",
            "#,##0.00")

#### Paragraph imd data
# write data to sheet
write_sheet(
  wb,
  "Table_9",
  "Table 9: Dependency Forming Medicines - England 2015/16 to 2021/22  Yearly patients, items and costs by Drug Category split by IMD Quintile",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. IMD Quintiles used are taken from the English Indices of Deprivation 2019 National Statistics publication.",
    "3. Where a patients lower-layer super output areas (LSOAs) is not available or has not been able to to be matched to National Statistics Postcode Lookup (May 2022) the records are reported as 'unknown' IMD Quintile.",
    "4. The patients lower-layer super output areas (LSOAs) is taken from electronic prescribing only meaning there are a higher number of patients registered as unknown in the earlier years when electronic prescribing was less prevalent.",
    "5. The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."

  ),
  workbook_data$category_imd,
  14
)
#left align columns A to C
format_data(wb,
            "Table_9",
            c("A", "B", "C"),
            "left",
            "")

#right align columns D and E and round to whole numbers with thousand separator
format_data(wb,
            "Table_9",
            c("D", "E"),
            "right",
            "#,##0")

#right align column F and round to 2dp with thousand separator
format_data(wb,
            "Table_9",
            c("F"),
            "right",
            "#,##0.00")
#### Coprescribing
# write data to sheet
write_sheet(
  wb,
  "Table_10",
  "Table 10: Dependency Forming Medicines - 2015/16 to 2021/22 - Monthly patients by number of categories of drugs recieved.",
  c(
    "1.The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  workbook_data$coprescribing,
  14
)
#left align columns A to C
format_data(wb,
            "Table_10",
            c("A", "B" ),
            "left",
            "")
#right align columns and round to 2 DP - D
format_data(wb,
            "Table_10",
            c("C"),
            "right",
            "#,##0")
#### Coprescribing combination
# write data to sheet
write_sheet(
  wb,
  "Table_11",
  "Table 11: Dependency Forming Medicines - 2015/16 to 2021/22 - Monthly patients by combination of drugs recieved for those recieving 2 drug categories.",
  c(
    "1.The patient counts shown in these statistics should only be analysed at the level at which they are presented. Adding together any patient counts is likely to result in an overestimate of the number of patients."
  ),
  workbook_data$coprescribing_combination,
  14
)
#left align columns A to C
format_data(wb,
            "Table_11",
            c("A", "B" ),
            "left",
            "")
#right align columns and round to 2 DP - D
format_data(wb,
            "Table_11",
            c("C"),
            "right",
            "#,##0")

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/dfm_2021_2022__patient_demographics_v001.xlsx",
                       overwrite = TRUE)
# 6. Automate QR table - tba

# 7. Automate Narrative - tba

# 8. render markdown ------------------------------------------------------
  rmarkdown::render("gphs-narrative-markdown.Rmd",
                    output_format = "html_document",
                    output_file = "outputs/gphs_annual_2021_22_v001.html")

  rmarkdown::render("gphs-narrative-markdown.Rmd",
                    output_format = "word_document",
                    output_file = "outputs/gphs_annual_2021_22_v001.docx")
