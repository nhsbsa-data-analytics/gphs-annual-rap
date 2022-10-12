# pipeline.R --------------------------------------------------------------
# This script is used to run the RAP for the dfm annual publication

# 1. install required packages --------------------------------------------
# TODO: investigate using renv package for dependency management
req_pkgs <- c("dplyr", "stringr", "data.table", "yaml", "openxlsx","rmarkdown",
              "logr", "highcharter", "lubridate", "dbplyr","tidyr","janitor", "DBI")


#uncomment if package installs are needed
#  utils::install.packages(req_pkgs, dependencies = TRUE)
#
devtools::install_github(
  "nhsbsa-data-analytics/gphsR",
  auth_token = Sys.getenv("GITHUB_PAT")
)

# devtools::install_github("nhsbsa-data-analytics/nhsbsaR")

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
# run functions for data and if needed write to csv

national_extract <- national_extract(
con = con,
table = "GPS_FINAL_DIM"
)
#write.csv(national_extract,"national_extract.csv")
national_month_extract <- national_month_extract(
  con = con,
  table = "GPS_MONTH_DIM"
)
#write.csv(national_month_extract,"national_month_extract.csv")
icb_extract <- icb_extract(
  con = con,
  table = "GPS_FINAL_DIM"
)
#write.csv(icb_extract,"icb_extract.csv")

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



#script for formatting the GPhS sumnmary tables using openxlsx
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
table_16 <- table_16(national_extract)
table_17 <- table_17(national_extract)
table_18 <- table_18(national_extract)
table_19 <- table_19(national_extract)
table_20 <- table_20()

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

wb <- create_wb(sheetNames)

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
  "Seasonal influenza vaccination advanced service"
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
"In 2015 community pharmacies began providing seasonal influenza vaccinations under a nationally commissioned service by NHS England & Improvement. Each year from September through to March, pharmacy contractors can administer flu vaccines to patients and submit a claim to NHSBSA for payment. This includes reimbursement of the cost of the vaccine, plus a fee for providing the service to NHS patients."
  )

create_metadata(wb,
                meta_fields,
                meta_descs
)

#### Table 1 national contractor totals
# write data to sheet
write_sheet(
  wb,
  "Table_1",
  "Table 1: General Pharmaceutical Services - 2015/16 to 2021/22 - Total number of Community pharmacies and appliance contractors",
  c(
    "1. Field definitions can be found on the 'Metadata' tab."
  ),
  table_1,
  42
)

#left align columns A (Financial Year)
format_data(wb,
            "Table_1",
            c("A"),
            "left",
            "")

#right align columns B to D and round to whole numbers with thousand separator
format_data(wb,
            "Table_1",
            c("B", "C", "D"),
            "right",
            "#,##0")

#### Table 2 pharmacy types
# write data to sheet
write_sheet(
  wb,
  "Table_2",
  "Table 2: General Pharmaceutical Services - 2015/16 to 2021/22 - Number of Pharmacies by attribute",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Data relates to community pharmacies that have submitted prescriptions to NHS Prescription Services for reimbursement at any point in the year",
    "3. A multiple contractor is defined as consisting of 6 pharmacies or more. Contractors with 5 pharmacies or less are regarded as independent contractors.",
    "4. The following abbreviations have been used in this table: Local Pharmacy Scheme (LPS)."
  ),
  table_2,
  14
)

#left align columns A
format_data(wb,
            "Table_2",
            c("A"),
            "left",
            "")

#right align columns B to H and round to whole numbers with thousand separator
format_data(wb,
            "Table_2",
            c("B", "C", "D", "E", "F", "G", "H"),
            "right",
            "#,##0")

#### Table 3
# write data to sheet
write_sheet(
  wb,
  "Table_3",
  "Table 3: General Pharmaceutical Services - 2015/16 to 2021/22 - Pharmacy activity items and cost",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The following abbreviations have been used in this table: Electronic Prescription Service (EPS)."
  ),
  table_3,
  14
)

#left align columns A
format_data(wb,
            "Table_3",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_3",
            c("B", "C", "F", "G"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_3",
            c("D", "E", "H"),
            "right",
            "#,##0.00")

#### Table 4
# write data to sheet
write_sheet(
  wb,
  "Table_4",
  "Table 4: General Pharmaceutical Services - 2015/16 to 2021/22 - Pharmacy activity by dispensing bands",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Data relates to community pharmacies that have submitted prescriptions to NHS Prescription Services for reimbursement at any point in the year.",
    "3. Monthly dispensing bands are calculated by dividing the total number of items dispensed in the year for each contractor by the number of months they were active in the year."
  ),
  table_4,
  14
)

#left align columns A
format_data(wb,
            "Table_4",
            c("A", "B"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_4",
            c("C", "D"),
            "right",
            "#,##0")

#### Table 5
# write data to sheet
write_sheet(
  wb,
  "Table_5",
  "Table 5: General Pharmaceutical Services - 2015/16 to 2021/22 - Pharmacy activity for essential services fees",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Data relates to community pharmacies that have submitted prescriptions to NHS Prescription Services for reimbursement at any point in the year.",
    "3. When a service was not available in a given financial year, these cells have been left blank.",
    "4. The Discharge Medicines Service (DMS) is an essential service that was introduced in February 2021. More information on DMS can be found on the 'Metadata' tab.",
    "5. The following abbreviations have been used in this table: Controlled Drug (CD), Discharge Medicines Service (DMS), Out Of Pocket Expenses fees (OOPE)."

  ),
  table_5,
  14
)

#left align columns A
format_data(wb,
            "Table_5",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_5",
            c("B", "C", "G", "I", "K", "M", "O", "Q", "R", "T", "U", "W"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_5",
            c("D", "E", "F", "H", "J", "L", "N", "P", "S", "V", "X"),
            "right",
            "#,##0.00")

#### Table 6
write_sheet(
  wb,
  "Table_6",
  "Table 6: General Pharmaceutical Services - 2015/16 to 2021/22 - Pharmacy activity for advanced services - Other",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Data relates to community pharmacies that have submitted prescriptions to NHS Prescription Services for reimbursement at any point in the year.",
    "3. When a service was not available in a given financial year, these cells have been left blank.",
    "4. The Hepatitis C testing service is an Advanced Service introduced in March 2021. More information on this service can be found on the 'Metadata' tab.",
    "5. The Hypertension Case-Finding service is an Advanced Service introduced in October 2021. More information on this service can be found on the 'Metadata' tab. ",
    "6. The Medicines Use Review (MUR) and Prescription Intervention Service was discontinued on 31 March 2021. More information on MUR can be found on the 'Metadata' tab.",
    "7. The following abbreviations have been used in this table: Hepatitis C (Hep C), Hypertension Case-Finding service (hypertension service), Medicines Use Review (MUR), New Medicine Service (NMS), Seasonal influenza vaccination advanced service (Flu vaccine)."
  ),
  table_6,
  14
)

#left align columns A
format_data(wb,
            "Table_6",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_6",
            c("B", "C", "D", "F", "G", "J", "M", "Q", "R"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_6",
            c("E", "H", "I", "K", "L", "N", "O", "P", "S"),
            "right",
            "#,##0.00")

#### Table 7
# write data to sheet
write_sheet(
  wb,
  "Table_7",
  "Table 7: General Pharmaceutical Services - 2015/16 to 2021/22 - Pharmacy activity for advanced services - AUR and SAC",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Data relates to community pharmacies that have submitted prescriptions to NHS Prescription Services for reimbursement at any point in the year.",
    "3. Appliance Use Reviews (AUR) - AURs can be carried out at either the patients home address, or at the pharmacy premises. Both pharmacy and appliance contractors can conduct AURs. See 'Metadata' tab for full definition.",
    "4. The following abbreviations have been used in this table: Appliance Use Review (AUR), Stoma Appliance Customisation (SAC)."
  ),
  table_7,
  14
)

#left align columns A
format_data(wb,
            "Table_7",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_7",
            c("B", "C", "D", "F", "G", "I", "J", "L"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_7",
            c("E", "H", "K", "M", "N"),
            "right",
            "#,##0.00")

#### Table 8
# write data to sheet
write_sheet(
  wb,
  "Table_8",
  "Table 8: General Pharmaceutical Services - 2015/16 to 2021/22 - Pharmacy activity for advanced services - CPCS",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The Community Pharmacist Consultation Service (CPCS) was introduced on 29 October 2019.",
    "3. When a service was not available in a given financial year, these cells have been left blank.",
    "4. The GP referral path service was introduced on 1 November 2020.",
    "5. The NHS Urgent Medicine Service (NUMSAS) has been integrated into CPCS and is no longer a separate service.",
    "6. The following abbreviations have been used in this table: Community Pharmacist Consultation Services (CPCS), GP referral pathway engagement fees (engagement fees), NHS Urgent Medicine Supply Advanced Service (NUMSAS)."
  ),
  table_8,
  14
)

#left align columns A
format_data(wb,
            "Table_8",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_8",
            c("B", "C", "D", "F", "H"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_8",
            c("D", "E", "G", "I", "J"),
            "right",
            "#,##0.00")


#### Table 9
# write data to sheet
write_sheet(
  wb,
  "Table_9",
  "Table 9: General Pharmaceutical Services - 2015/16 to 2021/22 - Pharmacy activity for COVID-19 related services",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. As these services were introduced in response to the COVID-19 pandemic, data is only available for financial year 2020/2021 onwards.",
    "3. When a service was not available in a given financial year, these cells have been left blank.",
    "4. The following abbreviations have been used in this table: Community Pharmacy Home Delivery Service (home deliveries), Personal Protective Equipment (PPE)."
  ),
  table_9,
  14
)

#left align columns A
format_data(wb,
            "Table_9",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_9",
            c("B", "C", "D", "F", "G", "J", "M", "Q", "R"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_9",
            c("E", "H", "I", "K", "L", "N", "O", "P", "S"),
            "right",
            "#,##0.00")

#### Table 10
# write data to sheet
write_sheet(
  wb,
  "Table_10",
  "Table 10: General Pharmaceutical Services - 2015/16 to 2021/22 - Appliance contractor activity items, cost and essential fees",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The following abbreviations have been used in this table: Electronic Prescription Service (EPS), Out Of Pocket Expenses (OOPE)."
  ),
  table_10,
  14
)

#left align columns A
format_data(wb,
            "Table_10",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_10",
            c("B", "C", "D", "G", "H", "J", "L", "M", "O", "P","R"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_10",
            c("E", "F", "I", "K", "N", "Q", "S"),
            "right",
            "#,##0.00")

#### Table 11
# write data to sheet
write_sheet(
  wb,
  "Table_11",
  "Table 11: General Pharmaceutical Services - 2015/16 to 2021/22 - Appliance contractor activity for advanced services - AUR and SAC",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The following abbreviations have been used in this table: Appliance Use Review (AUR), Stoma Appliance Customisation (SAC)."
  ),
  table_11,
  14
)

#left align columns A
format_data(wb,
            "Table_11",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_11",
            c("B", "C", "D", "F", "G", "I", "J", "L", "M"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_11",
            c("E", "H", "K", "N"),
            "right",
            "#,##0.00")

#### Table 12
# write data to sheet
write_sheet(
  wb,
  "Table_12",
  "Table 12: General Pharmaceutical Services - 2015/16 to 2021/22 - Pharmacy and appliance contractor activity items, cost and essential fees",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The following abbreviations have been used in this table: Electronic Prescription Service (EPS), Out Of Pocket Expenses (OOPE)."
  ),
  table_12,
  14
)

#left align columns A
format_data(wb,
            "Table_12",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_12",
            c("B", "C", "E", "F", "H", "J", "L", "M", "N", "O"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_12",
            c("D", "G", "I", "K", "N", "P", "Q"),
            "right",
            "#,##0.00")

#### Table 13
# write data to sheet
write_sheet(
  wb,
  "Table_13",
  "Table 13: General Pharmaceutical Services - 2015/16 to 2021/22 - Pharmacy and appliance contractor activity for advanced services",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The following abbreviations have been used in this table: Appliance Use Review (AUR), Stoma Appliance Customisation (SAC)."
  ),
  table_13,
  14
)

#left align columns A
format_data(wb,
            "Table_13",
            c("A"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_13",
            c("B", "C", "D", "F", "G", "I", "J", "L", "M"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_13",
            c("E", "H", "K", "N"),
            "right",
            "#,##0.00")

#### Table 14
# write data to sheet
write_sheet(
  wb,
  "Table_14",
  "Table 14: General Pharmaceutical Services - 2015/16 to 2021/22 - NHS England Regions - Community pharmacy contractors active during 2021/22",
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
format_data(wb,
            "Table_14",
            c("A", "B", "C"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_14",
            c("D", "E", "F", "G", "H", "I", "J"),
            "right",
            "#,##0")

#### Table 15
# write data to sheet
write_sheet(
  wb,
  "Table_15",
  "Table 15: General Pharmaceutical Services - 2015/16 to 2021/22 - Integrated Care Boards- Community pharmacy contractors active during 2021/22",
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
format_data(wb,
            "Table_15",
            c("A", "B", "C"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_15",
            c("D", "E", "F", "G", "H", "I", "J"),
            "right",
            "#,##0")

#### Table 16
# write data to sheet
write_sheet(
  wb,
  "Table_16",
  "Table 16: General Pharmaceutical Services - 2015/16 to 2021/22 - NHS England Regions - Services provided by community pharmacy contractors during 2021/22",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The data in this table relates to community pharmacies only, and excludes appliance contractors.",
    "3. The NHS England Regions shown here are reflective of the organisational structure as at 1 July 2022.",
    "4. The following abbreviations have been used in this table: Community Pharmacy Home Delivery Service (home deliveries), Community Pharmacist Consultation Services (CPCS), Electronic Prescription Service (EPS), New Medicine Service (NMS), Seasonal influenza vaccination advanced service (Flu vaccine)."
  ),
  table_16,
  14
)

#left align columns A, B, C
format_data(wb,
            "Table_16",
            c("A", "B", "C"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_16",
            c("D", "E", "F", "G", "I", "J", "L", "M", "P", "Q", "S"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_16",
            c("H", "K", "N", "O", "R", "T", "U", "V"),
            "right",
            "#,##0.00")

#### Table 17
# write data to sheet
write_sheet(
  wb,
  "Table_17",
  "Table 17: General Pharmaceutical Services - 2015/16 to 2021/22 - Integrated Care Boards - Services provided by community pharmacy contractors during 2021/22",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. The data in this table relates to community pharmacies only, and excludes appliance contractors.",
    "3. The Integrated Care Boards (ICBs) shown here are reflective of the organisational structure as of July 2022. ICBs succeeded Sustainability and Transformation Plans (STPs) in July 2022.",
    "4. The following abbreviations have been used in this table: Community Pharmacy Home Delivery Service (home deliveries), Community Pharmacist Consultation Services (CPCS), Electronic Prescription Service (EPS), New Medicine Service (NMS), Seasonal influenza vaccination advanced service (Flu vaccine)."
  ),
  table_17,
  14
)

#left align columns A, B, C
format_data(wb,
            "Table_17",
            c("A", "B", "C"),
            "left",
            "")

#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_17",
            c("D", "E", "F", "G", "I", "J", "L", "M", "P", "Q", "S"),
            "right",
            "#,##0")

#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_17",
            c("H", "K", "N", "O", "R", "T", "U", "V"),
            "right",
            "#,##0.00")

#### Table 18
# write data to sheet
write_sheet(
  wb,
  "Table_18",
  "Table 18: General Pharmaceutical Services - 2015/16 to 2021/22 - NHS England Regions -  Services provided by community pharmacies and appliance contractors during  2021/22",
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
format_data(wb,
            "Table_18",
            c("A","B","C"),
            "left",
            "")
#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_18",
            c("D", "E", "F", "G", "H", "I", "J", "N", "O"),
            "right",
            "#,##0")
#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_18",
            c("K", "L", "M", "P"),
            "right",
            "#,##0.00")

#### Table 19
# write data to sheet
write_sheet(
  wb,
  "Table_19",
  "Table 19: General Pharmaceutical Services - 2015/16 to 2021/22 - Integrated Care Boards - Services provided by community pharmacies and appliance contractors during 2021/22",
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
format_data(wb,
            "Table_19",
            c("A","B","C"),
            "left",
            "")
#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_19",
            c("D", "E", "F", "G", "H", "I", "J", "N", "O"),
            "right",
            "#,##0")
#right align column and round to 2dp with thousand separator
format_data(wb,
            "Table_19",
            c("K", "L", "M", "P"),
            "right",
            "#,##0.00")

#### Table 20
# write data to sheet
write_sheet(
  wb,
  "Table_20",
  "Table 20: General Pharmaceutical Services - Decisions on applications on appeal by decision, England 2013/14 to 2021/22",
  c(
    "1. More information on NHS Resolution and the data supplied here is available in the 'Background Information and Methodology' note that accompanies this release.",
    "2. Controlled / Non-controlled area - A controlled area is defined in the NHS Pharmaceutical and Local Pharmaceutical Regulations 2013 as one which NHS England has determined is rural in character. When NHS England receive applications to open a pharmacy in a controlled locality they consider whether granting the application would prejudice the provision of existing general medical or pharmaceutical
services in the locality."
  ),
table_20,
14
)
#left align columns A to C
format_data(wb,
            "Table_20",
            c("A","B","C"),
            "left",
            "")
#right align columns and round to whole numbers with thousand separator
format_data(wb,
            "Table_20",
            c("D"),
            "right",
            "#,##0")

#save file into outputs folder

openxlsx::saveWorkbook(wb,
                       "Y:/Official Stats/GPhS/Outputs/summary_tables_v003.xlsx",
                       overwrite = TRUE)
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
