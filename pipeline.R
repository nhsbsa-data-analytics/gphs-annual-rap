#pipeline.R
#this script provides the code to run the reproducible analytical pipeline
#and produce the General Pharmaceutical Services (GPhS) publication

#clear environment
rm(list = ls())

#source functions
source("./functions/functions.R")

#1. Setup and package installation
#load GITHUB_KEY if available in environment or enter if not

if (Sys.getenv("GITHUB_PAT") == "") {
  usethis::edit_r_environ()
  stop(
    "You need to set your GITHUB_PAT = YOUR PAT KEY in the .Renviron file which pops up. Please restart your R Studio after this and re-run the pipeline."
  )
}

#load GITHUB_KEY if available in environment or enter if not

#install nhsbsaUtils package first to use function check_and_install_packages()
devtools::install_github("nhsbsa-data-analytics/nhsbsaUtils",
                         auth_token = Sys.getenv("GITHUB_PAT"), force = TRUE)
#unload and reinstall gphsR if needed
#detach("package:gphsR", unload = TRUE)
#devtools::install_github("nhsbsa-data-analytics/gphsR",
#                         auth_token = Sys.getenv("GITHUB_PAT"), force = TRUE)



library(nhsbsaUtils)

#2. install required packages
#double check required packages once full pipeline built eg. if maps used
req_pkgs <- c("broom",
              "data.table",
              "devtools",
              "DBI",
              "dbplyr",
              "dplyr",
              "DT" ,
              "geojsonsf",
              "highcharter",
              "htmltools",
              "janitor",
              "kableExtra",
              "lubridate",
              "logr",
              "magrittr",
              "nhsbsa-data-analytics/nhsbsaR",
              "nhsbsa-data-analytics/gphsR",
              "nhsbsa-data-analytics/nhsbsaExternalData",
              "nhsbsa-data-analytics/accessibleTables",
              "nhsbsa-data-analytics/nhsbsaDataExtract",
              "nhsbsa-data-analytics/nhsbsaVis",
              "openxlsx",
              "rmarkdown",
              "rsample",
              "sf",
              "stringr",
              "tcltk",
              "tidyr",
              "tidyverse",
              "vroom",
              "yaml")

#library/install packages as required using nhsbsaUtils
nhsbsaUtils::check_and_install_packages(req_pkgs)

# set up logging
lf <-
  logr::log_open(paste0(
    "Y:/Official Stats/GPhS/log/gphs_log",
    format(Sys.time(), "%d%m%y%H%M%S"),
    ".log"
  ))

# load config
config <- yaml::yaml.load_file("config.yml")
log_print("Config loaded", hide_notes = TRUE)
log_print(config, hide_notes = TRUE)

# load options
nhsbsaUtils::publication_options()
log_print("Options loaded", hide_notes = TRUE)

#3. data import


#connect to datawarehouse
con <- nhsbsaR::con_nhsbsa(dsn = "FBS_8192k",
                           driver = "Oracle in OraClient19Home1",
                           "DWCP")

#get schema name for dataimport
username<- toupper(Sys.getenv("USERNAME"))

# run functions for data and if needed write to csv


national_extract <- national_extract(
  con = con,
  schema = username,
  table = "GPS_FINAL_202309_COMBINED"
)
#write.csv(national_extract,"national_extract.csv")
national_month_extract <- national_month_extract(
  con = con,
  schema = username,
  table = "GPS_MONTH_202309"
)
#write.csv(national_month_extract,"national_month_extract.csv")
icb_extract <- icb_extract(
  con = con,
  schema = username,
  table = "GPS_FINAL_202309_COMBINED"
)
#write.csv(icb_extract,"icb_extract.csv")


#4. create ref and set up tables

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
  ) %>%
# # compute final flu columns, replacing NAs with 0
 mutate(
   num_flu_pharm = num_flu_pharm_csv,
   flu_items_total =  flu_items_total_csv,
   flu_cost_total = flu_cost_total_csv,
   flu_fees_total = flu_fees_total_csv
 )


# set latest financial year
max_fyr <- "2022/2023"

#script for formatting the GPhS summary tables using openxlsx
#and existing formatting functions
#outputs an xlsx table

#run table functions and assign names, to get data to put in workbook
#for data arguments in functions use output of national_extract (with 2015/16 flu data)
#for tables 1 to 13
#or icb_extract for tables 14 to 19
#table 20 requires external Resolution data


table_1 <- table_1(national_extract)
table_2 <- table_2(national_extract)
table_3 <- table_3(national_extract)
table_4 <- table_4(national_extract)
table_5 <- table_5(national_extract)
table_6 <- table_6(national_extract)
table_7 <- table_7(national_extract)
table_8 <- table_8(national_extract)
table_9 <- table_9(national_extract)
table_10 <- table_10(national_extract)
table_11 <- table_11(national_extract)
table_12 <- table_12(national_extract)
table_13 <- table_13(national_extract)
table_14 <- table_14(icb_extract)
table_15 <- table_15(icb_extract)
table_16 <- table_16(icb_extract)
table_17 <- table_17(icb_extract)
table_18 <- table_18(icb_extract)
table_19 <- table_19(icb_extract)
table_20 <- table_20()



# 5. write data to .xlsx -
# create wb object
# create list of sheetnames needed (overview and metadata created automatically)
sheetNames <- c("Table_1",
                "Table_2",
                "Table_3",
                "Table_4",
                "Table_5",
                "Table_6",
                "Table_7",
                "Table_8",
                "Table_9",
                "Table_10",
                "Table_11",
                "Table_12",
                "Table_13",
                "Table_14",
                "Table_15",
                "Table_16",
                "Table_17",
                "Table_18",
                "Table_19",
                "Table_20")

wb <- accessibleTables::create_wb(sheetNames)

#create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "AUR",
  "Average monthly items per pharmacy",
  "Community Pharmacy Home Delivery Services",
  "Contractor Type",
  "Controlled / Non-controlled area",
  "Cost",
  "Covid-19 Lateral Flow Device Distribution Service",
  "COVID-19 vaccination enhanced service",
  "CPCS",
  "DMS",
  "EPS",
  "Fee",
  "Financial Year",
  "Hepatitis C testing service",
  "Hypertension Case-Finding",
  "Integrated Care Board Name",
  "Integrated Care Board Code",
  "Item",
  "MUR",
  "NHS England Region Name",
  "NHS England Region Code",
  "NMS",
  "SAC",
  "Seasonal influenza vaccination advanced service",
  "SCS"
)

meta_descs <-
  c(
    "Appliance Use Review (AUR) is the second Advanced Service to be introduced into the English Community Pharmacy Contractual Framework (CPCF). AURs can be carried out by a pharmacist or a specialist nurse in the pharmacy or at the patient’s home. AURs should improve the patient’s knowledge and use of any ‘specified appliance‘ by:

establishing the way the patient uses the appliance and the patient’s experience of such use;
identifying, discussing and assisting in the resolution of poor or ineffective use of the appliance by the patient;
advising the patient on the safe and appropriate storage of the appliance; and
advising the patient on the safe and proper disposal of the appliances that are used or unwanted.",
"This is calculated for each pharmacy by dividing the total items dispensed by the number of months the pharmacy was active in the year. The median of these figures is then calculated to give the final measure. A median is calculated by arranging all of the available values into an ordered list and selecting the value that is in the middle. If there are 2 middle values, the median is halfway between them. We use the median because the distribution of number of items dispensed is skewed, with a small number of contractors responsible for large volumes of dispensing on a monthly basis. When using the mean to calculate the average of a skewed distribution, it is highly influenced by those values at the upper end of the distribution and thus may not be truly representative. By taking the middle value of the data after sorting in ascending order the median avoids this issue.",
"Community Pharmacy Home Delivery Service was introduced during the COVID-19 pandemic. Initially used to deliver prescriptions to patients who were extremely clinically vulnerable and then to self-isolating patients. The data held does not differentiate between delivery services for CEV and those in self-isolation. The service ended on 31 March 2022.",
"Community pharmacies are typically responsible for dispensing drugs and medicines, and include high street pharmacies such as Boots and Lloyds. Appliance contractors are specialists in dispensing medical devices and appliances. A community pharmacy may dispense both medicines and appliances. However, an appliance contractor may only dispense medical devices and appliances. The figures for each of these type of contractor have been split out. Where contractor type is 'Pharmacy + Appliance' the figures for both contractor types have been combined, and do not indicate a separate type of contractor.",
"A controlled area is defined in the NHS Pharmaceutical and Local Pharmaceutical Regulations 2013 as one which NHS England has determined is rural in character. When NHS England receive applications to open a pharmacy in a controlled locality they consider whether granting the application would prejudice the provision of existing general medical or pharmaceutical services in the locality.",
"There are many costs incurred when a dispensing contractor fulfils a prescription. The costs reported in this publication represent the basic price of the item and the quantity prescribed. This is sometimes called the ‘Net Ingredient Cost’ (NIC). This also known as reimbursement of costs to dispensing contractors.",
"Covid-19 Lateral Flow Device Distribution Service - This Advanced Service was introduced in March 2021. It made lateral flow device (LFD) test kits readily available at community pharmacies. The service was part of the Government’s offer of lateral flow testing to all people in England and it worked alongside NHS Test and Trace’s other COVID-19 testing routes.",
"COVID-19 vaccination enhanced service - This enhanced service was introduced towards the end of 2020/21 allowing community pharmacies to provide COVID-19 vaccinations as part of the Phase 1 and Phase 2 cohorts of the Joint Committee on Vaccination and Immunisation (JCVI). A further enhanced service was commissioned to deliver Phase 3 alongside the seasonal influenza vaccination program.",
"Community Pharmacist Consultation Services - CPCS is an  Advanced Service introduced in October 2019 which allows general practices have been able to refer patients for a minor illness consultation via CPCS using an agreed local referral pathway.",
"Discharge Medicines Service - DMS is an essential service that was introduced in February 2021 and allows patients discharged from NHS Trust hospitals who need extra support with their medicines to be referred to their community pharmacy.",
"Electronic Prescription Service - EPS allows prescribers to send prescriptions electronically to a dispenser (such as a pharmacy) of the patient's choice. This makes the prescribing and dispensing process more efficient and convenient for patients and staff.",
"There are many fees that can be claimed by pharmacy and appliance contractors for providing essential and advanced services to NHS patients. The primary of these is the dispensing fee, which is also known as a professional fee or single activity fee. This fee is paid to a pharmacy or appliance contractor when they dispense a prescription item. Some items can attract more than one dispensing fee. Details of what fees are payable to pharmacy and appliance contractors can be found in the Drug Tariff for England and Wales.",
"The financial year to which the data belongs.",
"Hepatitis C testing service - This is an Advanced Service introduced in March 2021. The service offers people who inject drugs (PWIDs), who are not engaged with community drug and alcohol treatment services, opportunity to receive a point of care testing (POCT) HCV test from a community pharmacy.",
"Hypertension Case-Finding - This is an Advanced Service introduced in October 2021 following a NHS England pilot where pharmacies offered blood pressure checks to people 40 years and over and 24 hour ambulatory blood pressure monitoring (ABPM) to some patient’s with elevated initial blood pressure readings.",
"The name given to the Integrated Care Board (ICB) that a prescribing organisation belongs to. This is based upon NHSBSA administrative records, not geographical boundaries and more closely reflect the operational organisation of practices than other geographical data sources.",
"The unique code used to refer to an Integrated Care Board (ICB)",
"The number of prescription items dispensed. 'Items' is the number of times a product appears on a prescription form. Prescription forms include both paper prescriptions and electronic messages.",
"The Medicines Use Review (MUR) and Prescription Intervention Service was discontinued on 31 March 2021. It consisted of accredited pharmacists undertaking structured adherence-centred reviews with patients on multiple medicines, particularly those receiving medicines for long-term conditions. National target groups were agreed in order to guide the selection of patients the service was offered to.",
"The name given to the NHS England Regional team a prescribing organisation belongs to. This is based upon NHSBSA administrative records, not geographical boundaries and more closely reflect the operational organisation of practices than other geographical data sources.",
"The unique code used to refer to an NHS England Region.",
"The New Medicine Service (NMS) was the fourth Advanced Service to be added to the Community Pharmacy Contractual Framework; it commenced on 1st October 2011. The service provides support for people with long-term conditions newly prescribed a medicine to help improve medicines adherence. It is focused on particular patient groups and conditions.",
"Stoma Appliance Customisation (SAC) is the third Advanced service in the NHS community pharmacy contract. The service involves the customisation of a quantity of more than one stoma appliance, based on the patient’s measurements or a template. The aim of the service is to ensure proper use and comfortable fitting of the stoma appliance and to improve the duration of usage, thereby reducing waste. The stoma appliances that can be customised are listed in Part IXC of the Drug Tariff.",
"In 2015 community pharmacies began providing seasonal influenza vaccinations under a nationally commissioned service by NHS England & Improvement. Each year from September through to March, pharmacy contractors can administer flu vaccines to patients and submit a claim to NHSBSA for payment. This includes reimbursement of the cost of the vaccine, plus a fee for providing the service to NHS patients.",
"Smoking Cessation service (SCS) - This is an Advanced Service introduced in March 2022. Patients are discharged with consent to the community pharmacy of their choice to continue smoking cessation treatment began under an NHS Trust." )

accessibleTables::create_metadata(wb,
                                  meta_fields,
                                  meta_descs)
#### Table 1 national contractor totals
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_1",
  "Table 1: General Pharmaceutical Services - 2015/16 to 2022/23 - Total number of Community pharmacies and appliance contractors",
  c(
    "1. Field definitions can be found on the 'Metadata' tab."
  ),
  table_1,
  42
)

#left align columns A (Financial Year)
accessibleTables::format_data(wb,
            "Table_1",
            c("A"),
            "left",
            "")

#right align columns B to D and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_1",
            c("B", "C", "D"),
            "right",
            "#,##0")

#### Table 2 pharmacy types
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_2",
  "Table 2: General Pharmaceutical Services - 2015/16 to 2022/23 - Number of Pharmacies by attribute",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Data relates to community pharmacies that have submitted prescriptions to NHS Prescription Services for reimbursement at any point in the year",
    "3. A multiple contractor is defined as consisting of 6 pharmacies or more. Contractors with 5 pharmacies or less are regarded as independent contractors.",
    "4. The following abbreviations have been used in this table: Local Pharmacy Scheme (LPS)."
  ),
  table_2,
  23
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_2",
            c("A"),
            "left",
            "")

#right align columns B to H and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_2",
            c("B", "C", "D", "E", "F", "G", "H"),
            "right",
            "#,##0")

#### Table 3
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_3",
  "Table 3: General Pharmaceutical Services - 2015/16 to 2022/23 - Pharmacy activity items and cost",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The following abbreviations have been used in this table: Electronic Prescription Service (EPS)."
  ),
  table_3,
  14
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_3",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_3",
            c("B", "C", "F", "G"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_3",
            c("D", "E", "H"),
            "right",
            "#,##0.00")

#### Table 4
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_4",
  "Table 4: General Pharmaceutical Services - 2015/16 to 2022/23 - Pharmacy activity by dispensing bands",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Data relates to community pharmacies that have submitted prescriptions to NHS Prescription Services for reimbursement at any point in the year.",
    "3. Monthly dispensing bands are calculated by dividing the total number of items dispensed in the year for each contractor by the number of months they were active in the year."
  ),
  table_4,
  14
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_4",
            c("A", "B"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_4",
            c("C", "D"),
            "right",
            "#,##0")

#### Table 5
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_5",
  "Table 5: General Pharmaceutical Services - 2015/16 to 2022/23 - Pharmacy activity for essential services fees",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Data relates to community pharmacies that have submitted prescriptions to NHS Prescription Services for reimbursement at any point in the year.",
    "3. When a service was not available in a given financial year, these cells have been left blank.",
    "4. The Discharge Medicines Service (DMS) is an essential service that was introduced in February 2021. More information on DMS can be found on the 'Metadata' tab.",
    "5. The following abbreviations have been used in this table: Controlled Drug (CD), Discharge Medicines Service (DMS), Out Of Pocket Expenses fees (OOPE).",
    "6. An error was identified in the 2021/22 release of these tables, so the figures for 2021/22 for Discharge Medicines Service costs have been revised."

  ),
  table_5,
  14
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_5",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_5",
            c("B", "C", "G", "I", "K", "M", "O", "Q", "R", "T", "U", "W"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_5",
            c("D", "E", "F", "H", "J", "L", "N", "P", "S", "V", "X"),
            "right",
            "#,##0.00")

#### Table 6
accessibleTables::write_sheet(
  wb,
  "Table_6",
  "Table 6: General Pharmaceutical Services - 2015/16 to 2022/23 - Pharmacy activity for advanced services - Other",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Data relates to community pharmacies that have submitted prescriptions to NHS Prescription Services for reimbursement at any point in the year.",
    "3. When a service was not available in a given financial year, these cells have been left blank.",
    "4. The Hepatitis C testing service is an Advanced Service introduced in March 2021. More information on this service can be found on the 'Metadata' tab.",
    "5. The Hypertension Case-Finding service is an Advanced Service introduced in October 2021. More information on this service can be found on the 'Metadata' tab. ",
    "6. The Smoking Cessation service (SCS) is an Advanced Service introduced in March 2022. More information on this service can be found on the 'Metadata' tab. ",
    "7. The Medicines Use Review (MUR) and Prescription Intervention Service was discontinued on 31 March 2021. More information on MUR can be found on the 'Metadata' tab.",
    "8. The following abbreviations have been used in this table: Hepatitis C (Hep C), Hypertension Case-Finding service (hypertension service), Medicines Use Review (MUR), New Medicine Service (NMS), Seasonal influenza vaccination advanced service (Flu vaccine), Smoking Cessation service (SCS).",
    "9. An error was identified in the 2021/22 release of these tables, so the figures for 2021/22 for the Hypertension Case-Finding service and Hepatitis C testing and kit reimbursement service costs have been revised."

      ),
  table_6,
  14
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_6",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_6",
            c("B", "C", "D", "F", "G", "J", "M", "Q", "R", "U"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_6",
            c("E", "H", "I", "K", "L", "N", "O", "P", "R", "S", "T", "V", "W"),
            "right",
            "#,##0.00")

#### Table 7
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_7",
  "Table 7: General Pharmaceutical Services - 2015/16 to 2022/23 - Pharmacy activity for advanced services - AUR and SAC",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Data relates to community pharmacies that have submitted prescriptions to NHS Prescription Services for reimbursement at any point in the year.",
    "3. Appliance Use Reviews (AUR) - AURs can be carried out at either the patients home address, or at the pharmacy premises. Both pharmacy and appliance contractors can conduct AURs. See 'Metadata' tab for full definition.",
    "4. The following abbreviations have been used in this table: Appliance Use Review (AUR), Stoma Appliance Customisation (SAC).",
    "5. An error was identified in the 2021/22 release of these tables, so the figures for 2021/22 for the number of Appliance Use Reviews have been revised."
  ),
  table_7,
  14
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_7",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_7",
            c("B", "C", "D", "F", "G", "I", "J", "L"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_7",
            c("E", "H", "K", "M", "N"),
            "right",
            "#,##0.00")

#### Table 8
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_8",
  "Table 8: General Pharmaceutical Services - 2015/16 to 2022/23 - Pharmacy activity for advanced services - CPCS",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The Community Pharmacist Consultation Service (CPCS) was introduced on 29 October 2019.",
    "3. When a service was not available in a given financial year, these cells have been left blank.",
    "4. The GP referral path service was introduced on 1 November 2020 and had to be claimed by 30 June 2021.",
    "5. The NHS Urgent Medicine Service (NUMSAS) has been integrated into CPCS and is no longer a separate service.",
    "6. The following abbreviations have been used in this table: Community Pharmacist Consultation Services (CPCS), GP referral pathway engagement fees (engagement fees), NHS Urgent Medicine Supply Advanced Service (NUMSAS).",
    "7. An error was identified in the 2021/22 release of these tables, so the figures for 2021/22 for the cost of Community Pharmacist Consultation Services drugs and fees have been revised."
  ),
  table_8,
  14
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_8",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_8",
            c("B", "C", "D", "F", "H"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_8",
            c("D", "E", "G", "I", "J"),
            "right",
            "#,##0.00")


#### Table 9
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_9",
  "Table 9: General Pharmaceutical Services - 2015/16 to 2022/23 - Pharmacy activity for COVID-19 related services",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. As these services were introduced in response to the COVID-19 pandemic, data is only available for financial year 2020/2021 onwards.",
    "3. When a service was not available in a given financial year, these cells have been left blank.",
    "4. The following abbreviations have been used in this table: Community Pharmacy Home Delivery Service (home deliveries), Personal Protective Equipment (PPE).",
    "5. An error was identified in the 2021/22 release of these tables, so the figures for 2021/22 for the volume and cost of home deliveries as well as cost of COVID-19 Test Kit set up fees, Test Kit fees, COVID-19 vaccinations, COVID-19 premises and refrigeration costs have been revised."
  ),
  table_9,
  14
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_9",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_9",
            c("B", "C", "D", "F", "G", "I", "M", "Q", "R"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_9",
            c("E", "H", "J", "K", "L", "N", "O", "P", "S"),
            "right",
            "#,##0.00")

#### Table 10
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_10",
  "Table 10: General Pharmaceutical Services - 2015/16 to 2022/23 - Appliance contractor activity items, costs and essential fees",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The following abbreviations have been used in this table: Electronic Prescription Service (EPS), Out Of Pocket Expenses (OOPE)."
  ),
  table_10,
  14
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_10",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_10",
            c("B", "C", "E", "F", "G", "I", "L", "K", "O", "N","Q"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_10",
            c("D", "H", "J", "M", "P", "R", "S"),
            "right",
            "#,##0.00")

#### Table 11
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_11",
  "Table 11: General Pharmaceutical Services - 2015/16 to 2022/23 - Appliance contractor activity for advanced services - AUR and SAC",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The following abbreviations have been used in this table: Appliance Use Review (AUR), Stoma Appliance Customisation (SAC).",
    "5. An error was identified in the 2021/22 release of these tables, so the figures for 2021/22 for the number of Appliance Use Reviews have been revised."
  ),
  table_11,
  14
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_11",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_11",
            c("B", "C", "D", "F", "G", "I", "J", "L", "M"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_11",
            c("E", "H", "K", "N"),
            "right",
            "#,##0.00")

#### Table 12
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_12",
  "Table 12: General Pharmaceutical Services - 2015/16 to 2022/23 - Pharmacy and appliance contractor activity items, costs and essential fees",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The following abbreviations have been used in this table: Electronic Prescription Service (EPS), Out Of Pocket Expenses (OOPE)."
  ),
  table_12,
  14
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_12",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_12",
            c("B", "C", "E", "F", "H", "J", "L", "M", "N", "O", "P"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_12",
            c("D", "G", "I", "K", "N", "Q"),
            "right",
            "#,##0.00")

#### Table 13
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_13",
  "Table 13: General Pharmaceutical Services - 2015/16 to 2022/23 - Pharmacy and appliance contractor activity for advanced services",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The following abbreviations have been used in this table: Appliance Use Review (AUR), Stoma Appliance Customisation (SAC).",
    "3. An error was identified in the 2021/22 release of these tables, so the figures for 2021/22 for the number of Appliance Use Reviews have been revised."
  ),
  table_13,
  14
)

#left align columns A
accessibleTables::format_data(wb,
            "Table_13",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_13",
            c("B", "C", "D", "F", "G", "I", "J", "L", "M"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_13",
            c("E", "H", "K", "N"),
            "right",
            "#,##0.00")

#### Table 14
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_14",
  "Table 14: General Pharmaceutical Services - 2015/16 to 2022/23 - NHS England Regions - Community pharmacy contractors active during 2022/23",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The data in this table relates to community pharmacies only, and excludes appliance contractors.",
    "3. The NHS England Regions shown here are reflective of the organisational structure as at 1 July 2022.",
    "4. The following abbreviations have been used in this table: Local Pharmacy Scheme (LPS)."
  ),
  table_14,
  14
)

#left align columns A, B, C
accessibleTables::format_data(wb,
            "Table_14",
            c("A", "B", "C"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_14",
            c("D", "E", "F", "G", "H", "I", "J"),
            "right",
            "#,##0")

#### Table 15
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_15",
  "Table 15: General Pharmaceutical Services - 2015/16 to 2022/23 - Integrated Care Boards- Community pharmacy contractors active during 2022/23",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The data in this table relates to community pharmacies only, and excludes appliance contractors.",
    "3. The Integrated Care Boards (ICBs) shown here are reflective of the organisational structure as of July 2022. ICBs succeeded Sustainability and Transformation Plans (STPs) in July 2022.",
    "4. The following abbreviations have been used in this table: Local Pharmacy Scheme (LPS)."
  ),
  table_15,
  14
)

#left align columns A, B, C
accessibleTables::format_data(wb,
            "Table_15",
            c("A", "B", "C"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_15",
            c("D", "E", "F", "G", "H", "I", "J"),
            "right",
            "#,##0")

#### Table 16
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_16",
  "Table 16: General Pharmaceutical Services - 2015/16 to 2022/23 - NHS England Regions - Services provided by community pharmacy contractors during 2022/23",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The data in this table relates to community pharmacies only, and excludes appliance contractors.",
    "3. The NHS England Regions shown here are reflective of the organisational structure as at 1 July 2022.",
    "4. The following abbreviations have been used in this table:  Community Pharmacist Consultation Services (CPCS), Electronic Prescription Service (EPS), New Medicine Service (NMS), Seasonal influenza vaccination advanced service (Flu vaccine),  Hepatitis C (Hep C), Hypertension Case-Finding service (hypertension service), Medicines Use Review (MUR),  Smoking Cessation service (SCS)."
  ),
  table_16,
  14
)

#left align columns A, B, C
accessibleTables::format_data(wb,
            "Table_16",
            c("A", "B", "C"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_16",
            c("D", "E", "F", "G", "I", "J", "L", "M", "P", "Q", "S", "V", "Z", "AD"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_16",
            c("H", "K", "N", "O", "R", "T", "U", "W", "X", "Y", "AA", "AB", "AC"),
            "right",
            "#,##0.00")

#### Table 17
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_17",
  "Table 17: General Pharmaceutical Services - 2015/16 to 2022/23 - Integrated Care Boards - Services provided by community pharmacy contractors during 2022/23",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The data in this table relates to community pharmacies only, and excludes appliance contractors.",
    "3. The Integrated Care Boards (ICBs) shown here are reflective of the organisational structure as of July 2022. ICBs succeeded Sustainability and Transformation Plans (STPs) in July 2022.",
    "4. The following abbreviations have been used in this table:  Community Pharmacist Consultation Services (CPCS), Electronic Prescription Service (EPS), New Medicine Service (NMS), Seasonal influenza vaccination advanced service (Flu vaccine),  Hepatitis C (Hep C), Hypertension Case-Finding service (hypertension service), Medicines Use Review (MUR),  Smoking Cessation service (SCS)."
  ),
  table_17,
  14
)

#left align columns A, B, C
accessibleTables::format_data(wb,
            "Table_17",
            c("A", "B", "C"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_17",
            c("D", "E", "F", "G", "I", "J", "L", "M", "P", "Q", "S", "V", "Z", "AD"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_17",
            c("H", "K", "N", "O", "R", "T", "U", "W", "X", "Y", "AA", "AB", "AC"),
            "right",
            "#,##0.00")

#### Table 18
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_18",
  "Table 18: General Pharmaceutical Services - 2015/16 to 2022/23 - NHS England Regions -  Services provided by community pharmacies and appliance contractors during  2022/23",
  c(
    "1. The data in this table relates to both community pharmacies and appliance contractors.",
    "2. The NHS England Regions shown here are reflective of the organisational structure as of 1 July 2022.",
    "3. Appliance Use Reviews (AUR) - AURs can be carried out at either the patients home address, or at the pharmacy premises. Both pharmacy and appliance contractors can conduct AURs. See 'Metadata' tab for full definition.",
    "4. Both pharmacy and appliance contractors can provide Stoma Appliance Customisation (SAC) services. See 'Metadata' tab for full definition.",
    "5. The following abbreviations have been used in this table: Appliance Use Review (AUR), Stoma Appliance Customisation (SAC)."),
  table_18,
  14
)
#left align columns A to C C
accessibleTables::format_data(wb,
            "Table_18",
            c("A","B","C"),
            "left",
            "")
#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_18",
            c("D", "E", "F", "G", "H", "I", "J", "N", "O"),
            "right",
            "#,##0")
#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_18",
            c("K", "L", "M", "P"),
            "right",
            "#,##0.00")

#### Table 19
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_19",
  "Table 19: General Pharmaceutical Services - 2015/16 to 2022/23 - Integrated Care Boards - Services provided by community pharmacies and appliance contractors during 2022/23",
  c(
    "1. The data in this table relates to both community pharmacies and appliance contractors.",
    "2. The Integrated Care Boards (ICBs) shown here are reflective of the organisational structure as of July 2022. ICBs succeeded Sustainability and Transformation Plans (STPs) in July 2022.",
    "3. Appliance Use Reviews (AUR) - AURs can be carried out at either the patients home address, or at the pharmacy premises. Both pharmacy and appliance contractors can conduct AURs. See 'Metadata' tab for full definition.",
    "4. Both pharmacy and appliance contractors can provide Stoma Appliance Customisation (SAC) services. See 'Metadata' tab for full definition.",
    "5. The following abbreviations have been used in this table: Appliance Use Review (AUR), Stoma Appliance Customisation (SAC)."),
  table_19,
  14
)
#left align columns A to C
accessibleTables::format_data(wb,
            "Table_19",
            c("A","B","C"),
            "left",
            "")
#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_19",
            c("D", "E", "F", "G", "H", "I", "J", "N", "O"),
            "right",
            "#,##0")
#right align column and round to 2dp with thousand separator
accessibleTables::format_data(wb,
            "Table_19",
            c("K", "L", "M", "P"),
            "right",
            "#,##0.00")

#### Table 20
# write data to sheet
accessibleTables::write_sheet(
  wb,
  "Table_20",
  "Table 20: General Pharmaceutical Services - Decisions on applications on appeal by decision, England 2013/14 to 2022/23",
  c(
    "1. More information on NHS Resolution and the data supplied here is available in the 'Background Information and Methodology' note that accompanies this release.",
    "2. Controlled / Non-controlled area - as defined in the NHS Pharmaceutical and Local Pharmaceutical Regulations 2013. See 'Metadata' tab for full definition."
  ),
table_20,
14
)
#left align columns A to C
accessibleTables::format_data(wb,
            "Table_20",
            c("A","B","C"),
            "left",
            "")
#right align columns and round to whole numbers with thousand separator
accessibleTables::format_data(wb,
            "Table_20",
            c("D"),
            "right",
            "#,##0")

#create cover sheet
accessibleTables::makeCoverSheet(
  "General Pharmaceutical Services - England ",
  "Summary Statistics 2015/16 - 2022/23",
  "Publication Date: 12 October 2023",
  wb,
  sheetNames,
  c(
    "Metadata",
    "Table 1: Total number of Community pharmacies and appliance contractors between 2015/16 and 2022/23",
    "Table 2: Number of Pharmacies by attribute between 2015/16 and 2022/23",
    "Table 3: Pharmacy activity items and cost between 2015/16 and 2022/23",
    "Table 4: Pharmacy activity by dispensing bands between 2015/16 and 2022/23",
    "Table 5: Pharmacy activity for essential services fees between 2015/16 and 2022/23",
    "Table 6: Pharmacy activity for advance services - Other between 2015/16 and 2022/23",
    "Table 7: Pharmacy activity for advance services - AUR and SAC  between 2015/16 and 2022/23",
    "Table 8: Pharmacy activity for advance services - CPCS between 2015/16 and 2022/23",
    "Table 9: Pharmacy activity for COVID-19 related services between 2020/21 and 2022/23",
    "Table 10: Appliance contractor activity items, cost and essential fees between 2015/16 and 2022/23",
    "Table 11: Appliance contractor activity for advance services - AUR and SAC between 2015/16 and 2022/23",
    "Table 12: Pharmacy and appliance contractor activity items, cost and essential fees between 2015/16 and 2022/23",
    "Table 13: Pharmacy and appliance contractor activity for advance services - AUR and SAC between 2015/16 and 2022/23",
    "Table 14: NHS England Regions - Community pharmacy contractors active during  2022/23",
    "Table 15: Integrated Care Boards- Community pharmacy contractors active during  2022/23",
    "Table 16: NHS England Regions -  Services provided by community pharmacy contractors during  2022/23",
    "Table 17: Integrated Care Boards - Services provided by community pharmacy contractors during  2022/23",
    "Table 18: NHS England Regions -  Services provided by community pharmacies and appliance contractors during  2022/23",
    "Table 19: Integrated Care Boards - Services provided by community pharmacies and appliance contractors during  2022/23",
    "Table 20: Decisions on applications on appeal by decision, England 2022/23"

  ),
  c("Metadata", sheetNames)
)

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/gps_2223_summary_tables_v001.xlsx",
                       overwrite = TRUE)



# 6. data for chart and code for charts and tables

#table 1 saf fees

table_1_saf_data <- data.frame(feeName = c("Professional fee","Single activity fee","Single activity fee","Single activity fee","Single activity fee","Single activity fee","Single activity fee","Single activity fee"),
                        feeValue = c(90,113,125,129,126,127,129,127),
                        feeStart = c("01 Jan 2001","01 Dec 2016","01 Apr 2017","01 Nov 2017","01 Nov 2018","01 Aug 2019","01 Aug 2021","01 Apr 2022"),
                        feeEnd = c("30 Nov 2016","31 Mar 2017","31 Oct 2017","31 Oct 2018","31 Jul 2019","31 Jul 2021","31 Mar 2022","31 Mar 2023"))
table_1_data <- data.frame(FEE_NAME = c("Professional fee","Single activity fee","Single activity fee","Single activity fee","Single activity fee","Single activity fee","Single activity fee","Single activity fee"),
                           FEE_VALUE_PENCE = c(90,113,125,129,126,127,129,127),
                           FEE_START = c("01 Jan 2001","01 Dec 2016","01 Apr 2017","01 Nov 2017","01 Nov 2018","01 Aug 2019","01 Aug 2021","01 Apr 2022"),
                           FEE_END = c("30 Nov 2016","31 Mar 2017","31 Oct 2017","31 Oct 2018","31 Jul 2019","31 Jul 2021","31 Mar 2022","31 Mar 2023"))

table_1_saf<- DT::datatable(data = table_1_saf_data,
              rownames = FALSE,
              colnames = c("Fee name" = "feeName","Fee value (pence)" = "feeValue","Fee start date" = "feeStart","Fee end date" = "feeEnd"),
              options = list(dom = "t",
                             columnDefs = list(list(orderable = FALSE,
                                                    targets = '_all'))))
#figure 1 data - no of pharmacies
figure_1_data <- national_extract %>%
  dplyr::mutate(CONTRACTOR_TYPE =  case_when(APPLIANCE_DISPENSER_HIST == "Y" ~ "Appliance contractors",
                                             APPLIANCE_DISPENSER_HIST == "N" ~ "Community pharmacies")) %>%
  dplyr::select(FINANCIAL_YEAR,`Number of contractors` = num_contractors, CONTRACTOR_TYPE) %>%
  group_by(FINANCIAL_YEAR, CONTRACTOR_TYPE) %>%
  pivot_longer(cols = c(`Number of contractors`),
               names_to = "MEASURE",
               values_to = "VALUE")

#figure 1 chart
figure_1 <- figure_1_data%>%

  group_chart_hc_unround(
    x = FINANCIAL_YEAR,
    y = VALUE,
    group = CONTRACTOR_TYPE,
    type = "line",
    xLab = "Financial year",
    yLab = "Number of contractors",
    title = ""

  )
#figure 2 data no of items

figure_2_data <- national_extract %>%
  dplyr::mutate(CONTRACTOR_TYPE =  case_when(APPLIANCE_DISPENSER_HIST == "Y" ~ "Total items - appliance contractors",
                                             APPLIANCE_DISPENSER_HIST == "N" ~ "Total items - community pharmacies")) %>%
  dplyr::select(FINANCIAL_YEAR,`Total items` = items_total, CONTRACTOR_TYPE) %>%
  group_by(FINANCIAL_YEAR, CONTRACTOR_TYPE) %>%
  pivot_longer(cols = c(`Total items`),
               names_to = "MEASURE",
               values_to = "VALUE")
#figure 2 chart
figure_2 <- figure_2_data %>%
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_YEAR,
    y = VALUE,
    group = CONTRACTOR_TYPE,
    type = "line",
    xLab = "Financial year",
    yLab = "Number of items",
    title = ""

  )

#figure 3 data - average no of items

figure_3_data  <- national_extract %>%
  dplyr::mutate(CONTRACTOR_TYPE =  case_when(APPLIANCE_DISPENSER_HIST == "Y" ~ "Appliance contractor",
                                             APPLIANCE_DISPENSER_HIST == "N" ~ "Community pharmacy"),
                avg_items = items_total/num_contractors) %>%
  dplyr::select(FINANCIAL_YEAR,`Average items` = avg_items, CONTRACTOR_TYPE) %>%
  group_by(FINANCIAL_YEAR, `Average items`) %>%
  pivot_longer(cols = c(`Average items`),
               names_to = "MEASURE",
               values_to = "VALUE") %>%
  dplyr::mutate(VALUE = signif(VALUE, 3))

# figure 3 chart

figure_3 <- figure_3_data %>%
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_YEAR,
    y = VALUE,
    group = CONTRACTOR_TYPE,
    type = "line",
    xLab = "Financial year",
    yLab = "Number of items",
    title = "",
    dlOn = F
  ) %>%
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE)

#figure 4 eps data
figure_4_data  <- national_extract %>%
  dplyr::select(FINANCIAL_YEAR,items_total,eps_items_total_pharm) %>%
  dplyr::mutate(paper_items_all_cont = items_total - eps_items_total_pharm) %>%
  summarise(paper_items_all_cont = sum(paper_items_all_cont),
            eps_items_total_pharm = sum(eps_items_total_pharm),
            items_total = sum(items_total)) %>%
  tidyr::pivot_longer(cols = c(paper_items_all_cont,eps_items_total_pharm,items_total),
                      names_to = "MEASURE",
                      values_to = "VALUE") %>%
  dplyr::mutate(VALUE = signif(VALUE, 3)) %>%
  dplyr::mutate(
    MEASURE = case_when(MEASURE == "paper_items_all_cont" ~ "Items dispensed by paper prescription",
                        MEASURE == "eps_items_total_pharm" ~ "Items dispensed by EPS",
                        MEASURE == "items_total" ~ "All items dispensed")
  )

#figure 4 chart
figure_4 <- figure_4_data %>%
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_YEAR,
    y = VALUE,
    group = "MEASURE",
    type = "line",
    xLab = "Financial year",
    yLab = "Items dispensed",
    title = "",
    dlOn = F
  ) %>%
  hc_tooltip(enabled = TRUE,
             shared = TRUE,
             sort = TRUE)
#figure 5 costs
figure_5_data  <- national_extract %>%
  dplyr::select(FINANCIAL_YEAR,costs_total,prof_fees_pharm) %>%
  summarise(costs_total = sum(costs_total),
            prof_fees_pharm = sum(prof_fees_pharm)) %>%
  tidyr::pivot_longer(cols = c(costs_total,prof_fees_pharm),
                      names_to = "MEASURE",
                      values_to = "VALUE") %>%
  dplyr::mutate(
    MEASURE = case_when(MEASURE == "costs_total" ~ "Total reimbursement",
                        MEASURE == "prof_fees_pharm" ~ "Value of single activity fees")
  )
#figure 5 chart
figure_5 <- figure_5_data %>%
  nhsbsaVis::group_chart_hc(
    x = FINANCIAL_YEAR,
    y = VALUE,
    group = "MEASURE",
    type = "line",
    xLab = "Financial year",
    yLab = "Value (GBP)",
    title = "",
    currency = TRUE  )

#figure 6 flu

figure_6_data  <- national_extract %>%
  filter(APPLIANCE_DISPENSER_HIST == "N") %>%
  dplyr::select(FINANCIAL_YEAR,`FLU_VACCINES` = flu_items_total_csv) %>%
  summarise(`FLU_VACCINES` = sum(`FLU_VACCINES`))

#figure 6 chart
figure_6 <- figure_6_data %>%
  nhsbsaVis::basic_chart_hc(
    x = "FINANCIAL_YEAR",
    y = "FLU_VACCINES",
    type = "line",
    xLab = "Financial year",
    yLab = "Flu vaccines",
    title = ""
  )
# table 2 average flu per pharmacy
table_2_data <- national_extract %>%
  filter(APPLIANCE_DISPENSER_HIST == "N")%>%
  dplyr::select(FINANCIAL_YEAR,flu_items_total_csv,num_flu_pharm_csv) %>%
  summarise(flu_items_total_csv = sum(flu_items_total_csv),
            num_flu_pharm_csv = sum(num_flu_pharm_csv)) %>%
  dplyr::mutate(avg_flu_items = round(flu_items_total_csv/num_flu_pharm_csv,0)) %>%
  tidyr::pivot_longer(
    cols = c(flu_items_total_csv,num_flu_pharm_csv,avg_flu_items),
    names_to = "MEASURE",
    values_to = "VALUES"
  ) %>%
  filter(MEASURE == "avg_flu_items") %>%
  dplyr::mutate(
    MEASURE = case_when(MEASURE == "avg_flu_items" ~ "Average number of flu vaccines per community pharmacy")
  ) %>%
  pivot_wider(
    names_from = FINANCIAL_YEAR,
    values_from = VALUES
  )


table_2_flu <- DT::datatable(data = table_2_data,
              rownames = FALSE,
              colnames = c("Measure"="MEASURE"),
              options = list(dom = "t",
                             columnDefs = list(list(orderable = FALSE,
                                                    targets = '_all'))))
#figure 7 flu costs and fees

figure_7_data  <- national_extract %>%
  filter(APPLIANCE_DISPENSER_HIST == "N") %>%
  dplyr::select(FINANCIAL_YEAR,flu_cost_total_csv,flu_fees_total_csv) %>%
  summarise(flu_cost_total_csv = sum(flu_cost_total_csv),
            flu_fees_total_csv = sum(flu_fees_total_csv))  %>%
  tidyr::pivot_longer(cols = starts_with("flu_"),
                      names_to = "MEASURE",
                      values_to = "VALUES") %>%
  dplyr::mutate(
    MEASURE = case_when(MEASURE == "flu_cost_total_csv" ~ "Cost of vaccines",
                        MEASURE == "flu_fees_total_csv" ~ "Value of Fees received")
  )


#figure 7 chart
figure_7 <- figure_7_data %>%
  nhsbsaVis::group_chart_hc(
    x = "FINANCIAL_YEAR",
    y = "VALUES",
    group = "MEASURE",
    type = "column",
    xLab = "Financial year",
    yLab = "Value (GBP)",
    title = "",
    currency = TRUE
  ) %>%
  hc_plotOptions(column = list(stacking = "normal"))

#figure 8 nms volume

figure_8_data  <- national_extract %>%
  filter(APPLIANCE_DISPENSER_HIST == "N") %>%
  dplyr::select(FINANCIAL_YEAR,num_nms_total) %>%
  tidyr::pivot_longer(
    cols = c(num_nms_total),
    names_to = "MEASURE",
    values_to = "VALUES"
  ) %>%
  dplyr::mutate(
    MEASURE = case_when(MEASURE == "num_nms_total" ~ "Total New Medicine Services undertaken - community pharmacies")
  )

figure_8 <- figure_8_data %>%
  nhsbsaVis::group_chart_hc(
    x = "FINANCIAL_YEAR",
    y = "VALUES",
    group = "MEASURE",
    type = "line",
    xLab = "Financial year",
    yLab = "Total Advance Services",
    title = ""  )

#figure 9 nms cost

figure_9_data  <- national_extract %>%
  filter(APPLIANCE_DISPENSER_HIST == "N") %>%
  dplyr::select(FINANCIAL_YEAR,num_nms_fee_total) %>%
  tidyr::pivot_longer(
    cols = c(num_nms_fee_total),
    names_to = "MEASURE",
    values_to = "VALUES"
  ) %>%
  dplyr::mutate(
    MEASURE = case_when(MEASURE == "num_nms_fee_total" ~ "Total Cost of New Medicine Services undertaken - community pharmacies")
  )

#figure 9 chart
figure_9 <- figure_9_data %>%
  nhsbsaVis::group_chart_hc(
    x = "FINANCIAL_YEAR",
    y = "VALUES",
    group = "MEASURE",
    type = "line",
    xLab = "Financial year",
    yLab = "Value (GBP)",
    title ="",
    currency = TRUE
  )


#figure 10 aur volume

figure_10_data  <-
national_extract %>%
  dplyr::select(FINANCIAL_YEAR, num_aur_home_total_pharm,num_aur_prem_total_pharm,num_aur_all_total_pharm
  ) %>%
  summarise(num_aur_home_total_pharm = sum(num_aur_home_total_pharm),
            num_aur_prem_total_pharm = sum(num_aur_prem_total_pharm),
            num_aur_all_total_pharm = sum(num_aur_all_total_pharm))  %>%
  tidyr::pivot_longer(
    cols = c(num_aur_home_total_pharm,num_aur_prem_total_pharm,num_aur_all_total_pharm
    ),
    names_to = "MEASURE",
    values_to = "VALUES"
  ) %>%
  dplyr::mutate(
    MEASURE = case_when(MEASURE == "num_aur_home_total_pharm" ~ "Total Home Appliance Use Reviews (AUR)",MEASURE == "num_aur_prem_total_pharm" ~ "Total Premises Appliance Use Reviews (AUR)",MEASURE == "num_aur_all_total_pharm" ~ "Total All Appliance Use Reviews (AUR)"
    )
  )

#figure 10 chart
figure_10 <- figure_10_data %>%
  nhsbsaVis::group_chart_hc(
    x = "FINANCIAL_YEAR",
    y = "VALUES",
    group = "MEASURE",
    type = "line",
    xLab = "Financial year",
    yLab = "Total Advance Services",
    title = ""
  )

# figure 11 aur cost

figure_11_data <- national_extract %>%
  dplyr::select(FINANCIAL_YEAR, num_aur_home_total_fee_pharm,num_aur_prem_total_fee_pharm,num_aur_all_total_fee_pharm
  ) %>%
  summarise(num_aur_home_total_fee_pharm = sum(num_aur_home_total_fee_pharm),
            num_aur_prem_total_fee_pharm = sum(num_aur_prem_total_fee_pharm),
            num_aur_all_total_fee_pharm = sum(num_aur_all_total_fee_pharm))  %>%
  tidyr::pivot_longer(
    cols = c(num_aur_home_total_fee_pharm,num_aur_prem_total_fee_pharm,num_aur_all_total_fee_pharm
    ),
    names_to = "MEASURE",
    values_to = "VALUES"
  ) %>%
  dplyr::mutate(
    MEASURE = case_when(MEASURE == "num_aur_home_total_fee_pharm" ~ "Total Home Appliance Use Reviews (AUR)",MEASURE == "num_aur_prem_total_fee_pharm" ~ "Total Premises Appliance Use Reviews (AUR)",MEASURE == "num_aur_all_total_fee_pharm" ~ "Total All Appliance Use Reviews (AUR)"
    )
  )

#figure 11 chart
figure_11 <- figure_11_data %>%
  nhsbsaVis::group_chart_hc(
    x = "FINANCIAL_YEAR",
    y = "VALUES",
    group = "MEASURE",
    type = "line",
    xLab = "Financial year",
    yLab = "Value (GBP)",
    title = "",
    currency =TRUE
  )

# figure 12 sac volume

figure_12_data <- national_extract %>%
  dplyr::mutate(CONTRACTOR_TYPE =  case_when(APPLIANCE_DISPENSER_HIST == "Y" ~ "Total Stoma Appliance Customisation (SAC) provided by appliance contractors",
                                             APPLIANCE_DISPENSER_HIST == "N" ~ "Total Stoma Appliance Customisation (SAC) provided by community pharmacies")) %>%
  dplyr::select(FINANCIAL_YEAR,`Total Advance Services` = num_stoma_total_pharm, CONTRACTOR_TYPE) %>%
  group_by(FINANCIAL_YEAR, CONTRACTOR_TYPE) %>%

  tidyr::pivot_longer(
    cols = c(`Total Advance Services`),
    names_to = "MEASURE",
    values_to = "VALUES")

#figure 12 chart
figure_12 <- figure_12_data %>%
  nhsbsaVis::group_chart_hc(
    x = "FINANCIAL_YEAR",
    y = "VALUES",
    group = "CONTRACTOR_TYPE",
    type = "line",
    xLab = "Financial year",
    yLab = "Total Advance Services",
    title = "")
# figure 13 sac cost

figure_13_data <- national_extract %>%
  dplyr::mutate(CONTRACTOR_TYPE =  case_when(APPLIANCE_DISPENSER_HIST == "Y" ~ "Total Cost of Stoma Appliance Customisation (SAC) provided by appliance contractors",
                                             APPLIANCE_DISPENSER_HIST == "N" ~ "Total Cost of Stoma Appliance Customisation (SAC) provided by community pharmacies")) %>%
  dplyr::select(FINANCIAL_YEAR,`Cost of Advance Services fees` = num_stoma_total_fee_pharm, CONTRACTOR_TYPE) %>%
  group_by(FINANCIAL_YEAR, CONTRACTOR_TYPE) %>%

  tidyr::pivot_longer(
    cols = c(`Cost of Advance Services fees`),
    names_to = "MEASURE",
    values_to = "VALUES")

#figure 13 chart
figure_13 <- figure_13_data %>%
  nhsbsaVis::group_chart_hc(
    x = "FINANCIAL_YEAR",
    y = "VALUES",
    group = "CONTRACTOR_TYPE",
    type = "line",
    xLab = "Financial year",
    yLab = "Value (GBP)",
    title = "",
    currency = TRUE)

# figure 14 CSPC fee cost

figure_14_data <- national_extract %>%
  filter(APPLIANCE_DISPENSER_HIST == "N",
         FINANCIAL_YEAR  %!in% c("2015/2016","2016/2017","2017/2018","2018/2019")) %>%
  dplyr::select(FINANCIAL_YEAR, cpcs_fees_pharm) %>%
  tidyr::pivot_longer(
    cols = c(cpcs_fees_pharm),
    names_to = "MEASURE",
    values_to = "VALUES"
  ) %>%
  dplyr::mutate(
    MEASURE = case_when(MEASURE == "cpcs_fees_pharm" ~ "Total Cost of Community Pharmacy Consultation Services")
  )

#figure 14 chart
figure_14 <- figure_14_data %>%
  nhsbsaVis::group_chart_hc(
    x = "FINANCIAL_YEAR",
    y = "VALUES",
    group = "MEASURE",
    type = "line",
    xLab = "Financial Year",
    yLab = "Value (GBP)",
    title ="",
    currency = TRUE) %>%
  hc_xAxis(
    plotLines = list(
      list(
        value= 0,
        color = "grey",
        width = 1,
        dashStyle = "dash",
        label = list(
          rotation = 0,
          text = "<b>Note:</b> The figures <br>at this point <br> do not represent<br> the full financial year<br> and are for <br>October 2019 to <br>March 2020 only",
          style = list(
            fontSize = "10px"
          )
        )
      ))
  )

# figure 15 CSPC drug cost

figure_15_data <- national_extract %>%
  filter(APPLIANCE_DISPENSER_HIST == "N",
         FINANCIAL_YEAR  %!in% c("2015/2016","2016/2017","2017/2018","2018/2019")) %>%
  dplyr::select(FINANCIAL_YEAR, cpcs_drugs_pharm) %>%
  tidyr::pivot_longer(
    cols = c(cpcs_drugs_pharm),
    names_to = "MEASURE",
    values_to = "VALUES"
  ) %>%
  dplyr::mutate(
    MEASURE = case_when(MEASURE == "cpcs_drugs_pharm" ~ "Total Cost of Drugs provided during Community Pharmacy Consultation Services")
  )

#figure 15 chart
figure_15 <- figure_15_data %>%
  nhsbsaVis::group_chart_hc(
    x = "FINANCIAL_YEAR",
    y = "VALUES",
    group = "MEASURE",
    type = "line",
    xLab = "Financial Year",
    yLab = "Value (GBP)",
    title ="",
    currency = TRUE) %>%
  hc_xAxis(
    plotLines = list(
      list(
        value= 0,
        color = "grey",
        width = 1,
        dashStyle = "dash",
        label = list(
          rotation = 0,
          text = "<b>Note:</b> The figures <br>at this point <br> do not represent<br> the full financial year<br> and are for <br>October 2019 to <br>March 2020 only",
          style = list(
            fontSize = "10px"
          )
        )
      ))
  )

# figure 16 covid vaccine cost year

figure_16_data <-national_extract %>%
  filter(APPLIANCE_DISPENSER_HIST == "N",
         FINANCIAL_YEAR  %!in% c("2015/2016","2016/2017","2017/2018","2018/2019","2019/2020")) %>%
  dplyr::select(FINANCIAL_YEAR, CVD_19_VACCINE) %>%
                  summarise(CVD_19_VACCINE = sum(CVD_19_VACCINE))  %>%
                  tidyr::pivot_longer(cols = CVD_19_VACCINE,
                                      names_to = "MEASURE",
                                      values_to = "VALUES") %>%
                  dplyr::mutate(VALUES = signif(VALUES, 3)) %>%
                  dplyr::arrange(desc(MEASURE)) %>%

                  dplyr::mutate(
                    MEASURE = case_when(MEASURE == "CVD_19_VACCINE" ~ "Cost of vaccine fees")
                  )
#figure 16 chart
figure_16 <- figure_16_data %>%
  nhsbsaVis::group_chart_hc(
    x = "FINANCIAL_YEAR",
    y = "VALUES",
    group = "MEASURE",
    type = "line",
    xLab = "Financial Year",
    yLab = "Value (GBP)",
    title ="",
    currency = TRUE) %>%
  hc_xAxis(
    plotLines = list(
      list(
        value= 0,
        color = "grey",
        width = 1,
        dashStyle = "dash",
        label = list(
          rotation = 0,
          text = "<b>Note:</b> The figures <br>at this point <br> do not represent<br> the full financial year<br> and are for <br>December 2020 to <br>March 2021 only",
          style = list(
            fontSize = "10px"
          )
        )
      ))
  )


# 7. render markdown ------------------------------------------------------
  rmarkdown::render("gphs_annual_narrative_2223.Rmd",
                    output_format = "html_document",
                    output_file = "outputs/gphs_annual_2022_23_v001.html")

  rmarkdown::render("gphs_annual_narrative_2223.Rmd",
                    output_format = "word_document",
                    output_file = "outputs/gphs_annual_2022_23_v001.docx")

