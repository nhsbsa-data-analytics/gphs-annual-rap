# Extract Functions -------------------------------------------------------------
#'Extract ICB level data for GPhS
#'
#'Easy helper for 'national_extract' returns national level data from GPhs final fact table
#'
#' @param con the connection object
#' @param table the name of the GPhS fact table built in your schema
#'
#' @import nhsbsaR
#' @import dplyr
#' @import stringr
#'
#' @export
#'
#' @examples
#' icb_extract <- icb_extract(
# con = con,
# schema = "SCHEMA",
# table = "GPS_FINAL_202309"
#')



icb_extract <- function(con,
                        schema,
                        table) {

  fact <- dplyr::tbl(con,
                     from = table)  %>%
    collect() %>%
    # create any helper cols
    dplyr::mutate(
      # set dates in year 9999 to NA
      CLOSE_DATE_HIST = case_when(
        substr(CLOSE_DATE_HIST, 1, 4) == "9999" ~ as.character(NA),
        TRUE ~ as.character(CLOSE_DATE_HIST)
      ),
      START_DATE_HIST = case_when(
        substr(START_DATE_HIST, 1, 4) == "9999" ~ as.character(NA),
        TRUE ~ as.character(START_DATE_HIST)
      ),
      # create first and last day on fy helper cols
      last_day = as.Date(paste0(substr(FINANCIAL_YEAR,6,9),"0331"), format = "%Y%m%d"),
      first_day = as.Date(paste0(substr(FINANCIAL_YEAR,1,4),"0401"), format = "%Y%m%d"),
      # convert contractor start and end dates to datetime objects
      across(contains("DATE_HIST"), ~ as.Date(.x, format = "%d/%m/%Y")),
      # all AUR fees col
      AUR_ALL = AUR_HOME + AUR_PREM,
      # all AUF num col
      NUM_AUR_ALL = NUM_AUR_HOME + NUM_AUR_PREM
    ) %>%
    dplyr::group_by(FINANCIAL_YEAR,APPLIANCE_DISPENSER_HIST,ICB_CODE, ICB_NAME, REGION_CODE, REGION_NAME) %>%
    # summarise cols to reported measures
    dplyr::summarise(
      # # number of community pharmacies
      num_contractors = n(),
      # # total number of items dispensed
      items_total = sum(ITEMS, na.rm=TRUE),
      # # total nic(costs) of items dispensed
      costs_total = sum(NIC, na.rm=TRUE),
      # # total number of dispensing fees
      prof_fees_total_pharm = sum(NUM_PROF_FEES, na.rm=TRUE),
      # # value of dispensing fees
      prof_fees_pharm = sum(PROF_FEES, na.rm=TRUE),
      # # mean average monthly items
      avg_monthly_items_pharm = median(AVG_MONTHLY_ITEMS, na.rm=TRUE),
      # # num of contractors for dispensing bands
      band_items_0_2000 = sum(MONTHLY_DISP_VOL_BAND == "0 - 2000", na.rm=TRUE),
      band_items_2001_4000 = sum(MONTHLY_DISP_VOL_BAND == "2001 - 4000", na.rm=TRUE),
      band_items_4001_6000 = sum(MONTHLY_DISP_VOL_BAND == "4001 - 6000", na.rm=TRUE),
      band_items_6001_8000 = sum(MONTHLY_DISP_VOL_BAND == "6001 - 8000", na.rm=TRUE),
      band_items_8001_10000 = sum(MONTHLY_DISP_VOL_BAND == "8001 - 10000", na.rm=TRUE),
      band_items_10000 = sum(MONTHLY_DISP_VOL_BAND == "10000+", na.rm=TRUE),
      # items dispensed by contractors in dispensing bands
      vol_items_0_2000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "0 - 2000"], na.rm=TRUE),
      vol_items_2001_4000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "2001 - 4000"], na.rm=TRUE),
      vol_items_4001_6000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "4001 - 6000"], na.rm=TRUE),
      vol_items_6001_8000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "6001 - 8000"], na.rm=TRUE),
      vol_items_8001_10000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "8001 - 10000"], na.rm=TRUE),
      vol_items_10000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "10000+"], na.rm=TRUE),
      # # number of independent pharmacies
      num_ind_pharm = sum(CONTRACTOR_TYPE == "Independent", na.rm=TRUE),
      # # number of multiples
      num_multi_pharm = sum(CONTRACTOR_TYPE == "Multiple contractor", na.rm=TRUE),
      # # number of distance selling
      num_dist_selling_pharm = sum(DIST_SELLING_DISPENSER_HIST == "Y", na.rm=TRUE),
      # # number that opened in the year
      num_open_pharm = sum(START_DATE_HIST >= first_day, na.rm=TRUE),
      # # number that closed in the year
      num_close_pharm = sum(CLOSE_DATE_HIST <= last_day, na.rm=TRUE),
      # # number of lps contracts
      num_lps_pharm = sum(LPS_DISPENSER_HIST == "Y", na.rm=TRUE),
      # # number contractors that provided MURs
      num_mur_pharm = sum(NUM_MUR > 0, na.rm=TRUE),
      # # total number of MURs
      num_mur_total = sum(NUM_MUR, na.rm=TRUE),
      # # total of MUR fees
      num_mur_fee_total = sum(MUR_FEES, na.rm=TRUE),
      # # number of contractors that provided NMS
      num_nms_pharm = sum(NUM_NMS > 0, na.rm=TRUE),
      # # total number of NMS
      num_nms_total = sum(NUM_NMS, na.rm=TRUE),
      # # total of NMS fees
      num_nms_fee_total = sum(NMS_FEES, na.rm=TRUE),
      # # num contractors providing AUR home
      num_aur_home_pharm = sum(NUM_AUR_HOME > 0, na.rm=TRUE),
      # # num contractors providing AUR prem
      num_aur_prem_pharm = sum(NUM_AUR_PREM > 0, na.rm=TRUE),
      # # num of contractors providing any AUR
      num_aur_all_pharm = sum(NUM_AUR_ALL > 0, na.rm=TRUE),
      # # total AUR home
      num_aur_home_total_pharm = sum(NUM_AUR_HOME, na.rm=TRUE),
      # # total AUR prem
      num_aur_prem_total_pharm = sum(NUM_AUR_PREM, na.rm=TRUE),
      # # total AUR all
      num_aur_all_total_pharm = sum(NUM_AUR_ALL, na.rm=TRUE),
      # # total AUR home fee
      num_aur_home_total_fee_pharm = sum(AUR_HOME, na.rm=TRUE),
      # # total AUR prem fee
      num_aur_prem_total_fee_pharm = sum(AUR_PREM, na.rm=TRUE),
      # # total AUR all
      num_aur_all_total_fee_pharm = sum(AUR_ALL, na.rm=TRUE),
      # # number of contractors doing SAC
      num_stoma_pharm = sum(NUM_CUSTOM_STOMA_FEES > 0, na.rm=TRUE),
      # # total number of SAC
      num_stoma_total_pharm = sum(NUM_CUSTOM_STOMA_FEES, na.rm=TRUE),
      # # total number of SAC fee
      num_stoma_total_fee_pharm = sum(CUSTOM_STOMA_FEES, na.rm=TRUE),
      # # number of contractors providing home delivery
      num_home_del = sum(NUM_HOME_DEL > 0, na.rm=TRUE),
      # # total home delivery fees
      num_home_del_total = sum(NUM_HOME_DEL, na.rm=TRUE),
      # #total home delivery cost
      home_del_cost_total = sum(HOME_DEL_COST, na.rm=TRUE),
      # # number of contractors providing flu
      num_flu_pharm = sum(FLU_ITEMS > 0, na.rm=TRUE),
      # # total flu vaccines
      flu_items_total = sum(FLU_ITEMS, na.rm=TRUE),
      # # total flu costs
      flu_cost_total = sum(FLU_COST, na.rm=TRUE),
      # # total flu fees
      flu_fees_total = sum(FLU_FEES, na.rm=TRUE),
      # # number of contractors using EPS
      num_eps_pharm = sum(EPS_ITEMS > 0, na.rm=TRUE),
      # # total EPS items
      eps_items_total_pharm = sum(EPS_ITEMS, na.rm=TRUE),
      # # num of contractors providing cd_fee
      num_cd_fee = sum(CD_FEE > 0, na.rm=TRUE),
      # # total cost of  cd_fee
      num_cd_fee = sum(CD_FEE, na.rm=TRUE),
      # # num of contractors providing cd_sched2_fee
      num_cd_sched2_fee = sum(CD_SCHED2_FEE > 0, na.rm=TRUE),
      # # total cost of  cd_sched2_fee
      num_cd_sched2_fee = sum(CD_SCHED2_FEE, na.rm=TRUE),
      # # num of contractors providing cd_sched3_fee
      num_cd_sched3_fee = sum(CD_SCHED3_FEE > 0, na.rm=TRUE),
      # # total cost of  cd_sched3_fee
      num_cd_sched3_fee = sum(CD_SCHED3_FEE, na.rm=TRUE),
      # # num of contractors providing add_fee_2a
      num_add_fee_2a = sum(ADD_FEE_2A > 0, na.rm=TRUE),
      # # total cost of  add_fee_2a
      add_fee_2a= sum (ADD_FEE_2A, na.rm=TRUE),
      # # num of contractors providing add_fee_mf
      num_add_fee_mf_pharm = sum(ADD_FEE_MF > 0, na.rm=TRUE),
      # # total cost of  add_fee_mf
      add_fee_mf_pharm = sum(ADD_FEE_MF, na.rm=TRUE),
      # # num of contractors providing expensive_fee
      num_expensive_fees_pharm= sum(EXPENSIVE_FEE > 0, na.rm=TRUE),
      # # total number of  no_expensive_fees
      no_expensive_fees_pharm = sum(NO_EXPENSIVE_FEES, na.rm=TRUE),
      # # total cost of  expensive_fee
      expensive_fee_pharm = sum(EXPENSIVE_FEE, na.rm=TRUE),
      # # num of contractors providing oope_item_count
      num_oope_item_count_pharm = sum(OOPE_ITEM_COUNT > 0, na.rm=TRUE),
      # # total number of  oope_item_count
      oope_item_count_pharm = sum(OOPE_ITEM_COUNT, na.rm=TRUE),
      # # total cost of  oope_val
      oope_val_pharm = sum(OOPE_VAL, na.rm=TRUE),
      # # num of contractors providing ums_drugs
      num_ums_drugs_pharm = sum(UMS_DRUGS > 0, na.rm=TRUE),
      # # total fees cost of  ums_fees
      ums_fees_pharm = sum(UMS_FEES, na.rm=TRUE),
      # # total cost of  ums_drugs
      ums_drugs_pharm = sum(UMS_DRUGS, na.rm=TRUE),
      # # num of contractors providing cpcs_drugs
      num_cpcs_drugs_pharm = sum(CPCS_FEES > 0, na.rm=TRUE),
      # # total fees cost of  cpcs_fees
      cpcs_fees_pharm = sum(CPCS_FEES, na.rm=TRUE),
      # # total cost of  cpcs_drugs
      cpcs_drugs_pharm = sum(CPCS_DRUGS, na.rm=TRUE),
      # # num of contractors providing hep_c_service
      num_hep_c_service_pharm = sum(HEP_C_SERVICE > 0, na.rm=TRUE),
      # # total cost of  hep_c_kit
      hep_c_kit_pharm = sum(HEP_C_KIT, na.rm=TRUE),
      # # total cost of  hep_c_service
      hep_c_service_pharm = sum(HEP_C_SERVICE, na.rm=TRUE),
      # # num of contractors providing hypertension incentives
      NUM_HYPTEN_INC= sum(HYPTEN_INC>0, na.rm=TRUE),
      # # total cost of  hypertension check fees
      HYPTENCHECK_FEES= sum(HYPTENCHECK_FEES, na.rm=TRUE),
      # # total cost of  hypertension set up fees
      HYPTENSET_FEES= sum(HYPTENSET_FEES, na.rm=TRUE),
      # # total cost of  hypertension incentives
      HYPTEN_INC= sum(HYPTEN_INC, na.rm=TRUE),
      # # num of contractors providing smoking cessation consultations
      NUM_SCS_CNSLT= sum(SCS_CNSLT>0, na.rm=TRUE),
      # # total cost of  smoking cessation consultations
      SCS_CNSLT= sum(SCS_CNSLT, na.rm=TRUE),
      # # total cost of  smoking cessation product charges
      SCS_NRTPROD_CHARGE = sum(SCS_NRTPROD_CHARGE, na.rm=TRUE),
      # # total cost of  smoking cessation product costs
      SCS_NRTPROD_COST= sum(SCS_NRTPROD_COST, na.rm=TRUE),
      # # total cost of  smoking cessation set up
      SCS_SETUP= sum(SCS_SETUP, na.rm=TRUE),
      # # num of contractors  serious shortage protocols
      NUM_SSP_FEES= sum(SSP_FEES>0, na.rm=TRUE),
      # # total cost of  serious shortage protocols
      SSP_FEES= sum(SSP_FEES, na.rm=TRUE),
      # # num of contractors receiving GP referral path fees
      NUM_GPREFPATH_FEES= sum(GPREFPATH_FEES>0, na.rm=TRUE),
      # # total cost of GP referral path fees
      GPREFPATH_FEES= sum(GPREFPATH_FEES, na.rm=TRUE),
      # # num of contractors receiving covid test reg fees
      NUM_CVDTESTREG_FEES= sum(CVDTESTREG_FEES>0, na.rm=TRUE),
      # # total cost of covid test reg fees
      CVDTESTREG_FEES= sum(CVDTESTREG_FEES, na.rm=TRUE),
      # # num of contractors receiving covid test set up fees
      NUM_CVDTESTSET_FEES= sum(CVDTESTSET_FEES>0, na.rm=TRUE),
      # # total cost of covid test set upfees
      CVDTESTSET_FEES= sum(CVDTESTSET_FEES, na.rm=TRUE),
      # # num of contractors receiving covid test kits
      NUM_CVDTEST_KITS= sum(CVDTEST_KITS>0, na.rm=TRUE),
      # # total cost of covid test kits
      CVDTEST_KITS= sum(CVDTEST_KITS, na.rm=TRUE),
      # # num of contractors receiving covid 19 costs
      NUM_CVD_19_COSTS= sum(CVD_19_COSTS>0, na.rm=TRUE),
      # # total cost of covid 19 costs
      CVD_19_COSTS= sum(CVD_19_COSTS, na.rm=TRUE),
      # # num of contractors receiving covid 19 vaccine
      NUM_CVD_19_VACCINE = sum(CVD_19_VACCINE>0, na.rm=TRUE),
      # # total cost of covid 19 vaccine
      CVD_19_VACCINE = sum(CVD_19_VACCINE, na.rm=TRUE),
      # # num of contractors receiving covid 19 premises refrigeration
      NUM_CVD_PREM_REFRID= sum(CVD_PREM_REFRID>0, na.rm=TRUE),
      # # total cost of covid 19 premises refrigeration
      CVD_PREM_REFRID= sum(CVD_PREM_REFRID, na.rm=TRUE),
      # # num of contractors receiving discharge meds
      NUM_DISCHARGE_MEDS= sum(DISCHARGE_MEDS>0, na.rm=TRUE),
      # # total cost of covid 19 discharge meds
      DISCHARGE_MEDS= sum(DISCHARGE_MEDS, na.rm=TRUE),
      # # num of contractors receiving ppe claims
      NUM_PPE_CLAIMS= sum(PPE_CLAIMS>0, na.rm=TRUE),
      # # total cost of covid 19 PPE claims
      PPE_CLAIMS= sum(PPE_CLAIMS, na.rm=TRUE),
      # # num of contractors providing tier 1 contraceptive consultations
      num_t1c_cnslt= sum(T1C_CONSULT>0, na.rm=TRUE),
      # # total cost of tier 1 contraceptive set up
      t1c_set_up = sum(T1C_SET_UP,na.rm=TRUE),
      # # total cost of tier 1 contraceptive product
      t1c_prod_cost = sum(T1C_PROD_COST,na.rm=TRUE),
      # # total cost of tier 1 contraceptive consults
      t1c_consult = sum(T1C_CONSULT,na.rm=TRUE),
      # # num of contractors receiving pharmacy first payments
      num_pfcp_payment= sum(PFCP_PAYMENT>0, na.rm=TRUE),
      # # total cost of pharmacy first opt in
      pfcp_optin = sum(PFCP_OPTIN,na.rm=TRUE),
      # # total cost of pharmacy first fees
      pfcp_fees = sum(PFCP_FEES,na.rm=TRUE),
      # # total cost of pharmacy first opt payment
      pfcp_payment = sum(PFCP_PAYMENT,na.rm=TRUE),
      # # total cost of pharmacy first months
      pfcp_months = sum(PFCP_MONTHS,na.rm=TRUE),
      # # total cost of pharmacy first VAT
      pfcp_vat = sum(PFCP_VAT,na.rm=TRUE),
      # # total cost of pharmacy first deductions
      pfcp_umsmideduct = sum(PFCP_UMSMIDEDUCT,na.rm=TRUE),
      # # total cost of pharmacy first remuneration
      pfcp_umsmiremuneration = sum(PFCP_UMSMIREMUNERATION,na.rm=TRUE),
      # # total cost of pharmacy first reimbursent
      pfcp_umsmireimbursement = sum(PFCP_UMSMIREIMBURSEMENT,na.rm=TRUE)
    ) %>%
    dplyr::arrange(FINANCIAL_YEAR, APPLIANCE_DISPENSER_HIST) %>%
    collect()



  return(fact)

}

#'Extract national level data for GPhS
#'
#'Easy helper for 'national_month_extract' returns national level data from GPhs final fact table monthly for most recent year
#'
#'
#' @param con the connection object
#' @param table the name of the GPhS fact table built in your schema
#'
#' @import nhsbsaR
#' @import dplyr
#' @import stringr
#'
#' @export
#'
#' @examples
#' national_month_extract <- national_month_extract(
# con = con,
# schema = "SCHEMA",
# table = "GPS_FINAL_202309"
#')


national_month_extract <- function(con,
                                   schema,
                                   table) {

  fact <- dplyr::tbl(con,
                     from = table)  %>%
    collect() %>%
    # create any helper cols
    dplyr::mutate(
      # set dates in year 9999 to NA
      CLOSE_DATE_HIST = case_when(
        substr(CLOSE_DATE_HIST, 1, 4) == "9999" ~ as.character(NA),
        TRUE ~ as.character(CLOSE_DATE_HIST)
      ),
      START_DATE_HIST = case_when(
        substr(START_DATE_HIST, 1, 4) == "9999" ~ as.character(NA),
        TRUE ~ as.character(START_DATE_HIST)
      ),
      # create first and last day on fy helper cols
      last_day = as.Date(paste0(substr(YEAR_MONTH,1,4),"0331"), format = "%Y%m%d"),
      first_day = as.Date(paste0(substr(YEAR_MONTH,1,4),"0401"), format = "%Y%m%d"),
      # convert contractor start and end dates to datetime objects
      across(contains("DATE_HIST"), ~ as.Date(.x, format = "%d/%m/%Y")),
      # all AUR fees col
      AUR_ALL = AUR_HOME + AUR_PREM,
      # all AUF num col
      NUM_AUR_ALL = NUM_AUR_HOME + NUM_AUR_PREM
    ) %>%
    dplyr::group_by(YEAR_MONTH,APPLIANCE_DISPENSER_HIST) %>%
    # summarise cols to reported measures
    dplyr::summarise(
      # # number of community pharmacies
      num_contractors = n(),
      # # total number of items dispensed
      items_total = sum(ITEMS, na.rm=TRUE),
      # # total nic(costs) of items dispensed
      costs_total = sum(NIC, na.rm=TRUE),
      # # total number of dispensing fees
      prof_fees_total_pharm = sum(NUM_PROF_FEES, na.rm=TRUE),
      # # value of dispensing fees
      prof_fees_pharm = sum(PROF_FEES, na.rm=TRUE),
      # # mean average monthly items
      avg_monthly_items_pharm = median(AVG_MONTHLY_ITEMS, na.rm=TRUE),
      # # num of contractors for dispensing bands
      band_items_0_2000 = sum(MONTHLY_DISP_VOL_BAND == "0 - 2000", na.rm=TRUE),
      band_items_2001_4000 = sum(MONTHLY_DISP_VOL_BAND == "2001 - 4000", na.rm=TRUE),
      band_items_4001_6000 = sum(MONTHLY_DISP_VOL_BAND == "4001 - 6000", na.rm=TRUE),
      band_items_6001_8000 = sum(MONTHLY_DISP_VOL_BAND == "6001 - 8000", na.rm=TRUE),
      band_items_8001_10000 = sum(MONTHLY_DISP_VOL_BAND == "8001 - 10000", na.rm=TRUE),
      band_items_10000 = sum(MONTHLY_DISP_VOL_BAND == "10000+", na.rm=TRUE),
      # items dispensed by contractors in dispensing bands
      vol_items_0_2000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "0 - 2000"], na.rm=TRUE),
      vol_items_2001_4000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "2001 - 4000"], na.rm=TRUE),
      vol_items_4001_6000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "4001 - 6000"], na.rm=TRUE),
      vol_items_6001_8000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "6001 - 8000"], na.rm=TRUE),
      vol_items_8001_10000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "8001 - 10000"], na.rm=TRUE),
      vol_items_10000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "10000+"], na.rm=TRUE),
      # # number of independent pharmacies
      num_ind_pharm = sum(CONTRACTOR_TYPE == "Independent", na.rm=TRUE),
      # # number of multiples
      num_multi_pharm = sum(CONTRACTOR_TYPE == "Multiple contractor", na.rm=TRUE),
      # # number of distance selling
      num_dist_selling_pharm = sum(DIST_SELLING_DISPENSER_HIST == "Y", na.rm=TRUE),
      # # number that opened in the year
      num_open_pharm = sum(START_DATE_HIST >= first_day, na.rm=TRUE),
      # # number that closed in the year
      num_close_pharm = sum(CLOSE_DATE_HIST <= last_day, na.rm=TRUE),
      # # number of lps contracts
      num_lps_pharm = sum(LPS_DISPENSER_HIST == "Y", na.rm=TRUE),
      # # number contractors that provided MURs
      num_mur_pharm = sum(NUM_MUR > 0, na.rm=TRUE),
      # # total number of MURs
      num_mur_total = sum(NUM_MUR, na.rm=TRUE),
      # # total of MUR fees
      num_mur_fee_total = sum(MUR_FEES, na.rm=TRUE),
      # # number of contractors that provided NMS
      num_nms_pharm = sum(NUM_NMS > 0, na.rm=TRUE),
      # # total number of NMS
      num_nms_total = sum(NUM_NMS, na.rm=TRUE),
      # # total of NMS fees
      num_nms_fee_total = sum(NMS_FEES, na.rm=TRUE),
      # # num contractors providing AUR home
      num_aur_home_pharm = sum(NUM_AUR_HOME > 0, na.rm=TRUE),
      # # num contractors providing AUR prem
      num_aur_prem_pharm = sum(NUM_AUR_PREM > 0, na.rm=TRUE),
      # # num of contractors providing any AUR
      num_aur_all_pharm = sum(NUM_AUR_ALL > 0, na.rm=TRUE),
      # # total AUR home
      num_aur_home_total_pharm = sum(NUM_AUR_HOME, na.rm=TRUE),
      # # total AUR prem
      num_aur_prem_total_pharm = sum(NUM_AUR_PREM, na.rm=TRUE),
      # # total AUR all
      num_aur_all_total_pharm = sum(NUM_AUR_ALL, na.rm=TRUE),
      # # total AUR home fee
      num_aur_home_total_fee_pharm = sum(AUR_HOME, na.rm=TRUE),
      # # total AUR prem fee
      num_aur_prem_total_fee_pharm = sum(AUR_PREM, na.rm=TRUE),
      # # total AUR all
      num_aur_all_total_fee_pharm = sum(AUR_ALL, na.rm=TRUE),
      # # number of contractors doing SAC
      num_stoma_pharm = sum(NUM_CUSTOM_STOMA_FEES > 0, na.rm=TRUE),
      # # total number of SAC
      num_stoma_total_pharm = sum(NUM_CUSTOM_STOMA_FEES, na.rm=TRUE),
      # # total number of SAC fee
      num_stoma_total_fee_pharm = sum(CUSTOM_STOMA_FEES, na.rm=TRUE),
      # # number of contractors providing home delivery
      num_home_del = sum(NUM_HOME_DEL > 0, na.rm=TRUE),
      # # total home delivery fees
      num_home_del_total = sum(NUM_HOME_DEL, na.rm=TRUE),
      # #total home delivery cost
      home_del_cost_total = sum(HOME_DEL_COST, na.rm=TRUE),
      # # number of contractors providing flu
      num_flu_pharm = sum(FLU_ITEMS > 0, na.rm=TRUE),
      # # total flu vaccines
      flu_items_total = sum(FLU_ITEMS, na.rm=TRUE),
      # # total flu costs
      flu_cost_total = sum(FLU_COST, na.rm=TRUE),
      # # total flu fees
      flu_fees_total = sum(FLU_FEES, na.rm=TRUE),
      # # number of contractors using EPS
      num_eps_pharm = sum(EPS_ITEMS > 0, na.rm=TRUE),
      # # total EPS items
      eps_items_total_pharm = sum(EPS_ITEMS, na.rm=TRUE),
      # # num of contractors providing cd_fee
      num_cd_fee = sum(CD_FEE > 0, na.rm=TRUE),
      # # total cost of  cd_fee
      num_cd_fee = sum(CD_FEE, na.rm=TRUE),
      # # num of contractors providing cd_sched2_fee
      num_cd_sched2_fee = sum(CD_SCHED2_FEE > 0, na.rm=TRUE),
      # # total cost of  cd_sched2_fee
      num_cd_sched2_fee = sum(CD_SCHED2_FEE, na.rm=TRUE),
      # # num of contractors providing cd_sched3_fee
      num_cd_sched3_fee = sum(CD_SCHED3_FEE > 0, na.rm=TRUE),
      # # total cost of  cd_sched3_fee
      num_cd_sched3_fee = sum(CD_SCHED3_FEE, na.rm=TRUE),
      # # num of contractors providing add_fee_2a
      num_add_fee_2a = sum(ADD_FEE_2A > 0, na.rm=TRUE),
      # # total cost of  add_fee_2a
      add_fee_2a= sum (ADD_FEE_2A, na.rm=TRUE),
      # # num of contractors providing add_fee_mf
      num_add_fee_mf_pharm = sum(ADD_FEE_MF > 0, na.rm=TRUE),
      # # total cost of  add_fee_mf
      add_fee_mf_pharm = sum(ADD_FEE_MF, na.rm=TRUE),
      # # num of contractors providing expensive_fee
      num_expensive_fees_pharm= sum(EXPENSIVE_FEE > 0, na.rm=TRUE),
      # # total number of  no_expensive_fees
      no_expensive_fees_pharm = sum(NO_EXPENSIVE_FEES, na.rm=TRUE),
      # # total cost of  expensive_fee
      expensive_fee_pharm = sum(EXPENSIVE_FEE, na.rm=TRUE),
      # # num of contractors providing oope_item_count
      num_oope_item_count_pharm = sum(OOPE_ITEM_COUNT > 0, na.rm=TRUE),
      # # total number of  oope_item_count
      oope_item_count_pharm = sum(OOPE_ITEM_COUNT, na.rm=TRUE),
      # # total cost of  oope_val
      oope_val_pharm = sum(OOPE_VAL, na.rm=TRUE),
      # # num of contractors providing ums_drugs
      num_ums_drugs_pharm = sum(UMS_DRUGS > 0, na.rm=TRUE),
      # # total fees cost of  ums_fees
      ums_fees_pharm = sum(UMS_FEES, na.rm=TRUE),
      # # total cost of  ums_drugs
      ums_drugs_pharm = sum(UMS_DRUGS, na.rm=TRUE),
      # # num of contractors providing cpcs_drugs
      num_cpcs_drugs_pharm = sum(CPCS_DRUGS > 0, na.rm=TRUE),
      # # total fees cost of  cpcs_fees
      cpcs_fees_pharm = sum(CPCS_FEES, na.rm=TRUE),
      # # total cost of  cpcs_drugs
      cpcs_drugs_pharm = sum(CPCS_DRUGS, na.rm=TRUE),
      # # num of contractors providing hep_c_service
      num_hep_c_service_pharm = sum(HEP_C_SERVICE > 0, na.rm=TRUE),
      # # total cost of  hep_c_kit
      hep_c_kit_pharm = sum(HEP_C_KIT, na.rm=TRUE),
      # # total cost of  hep_c_service
      hep_c_service_pharm = sum(HEP_C_SERVICE, na.rm=TRUE),
      # # num of contractors providing hypertension incentives
      NUM_HYPTEN_INC= sum(HYPTEN_INC>0, na.rm=TRUE),
      # # total cost of  hypertension check fees
      HYPTENCHECK_FEES= sum(HYPTENCHECK_FEES, na.rm=TRUE),
      # # total cost of  hypertension set up fees
      HYPTENSET_FEES= sum(HYPTENSET_FEES, na.rm=TRUE),
      # # total cost of  hypertension incentives
      HYPTEN_INC= sum(HYPTEN_INC, na.rm=TRUE),
      # # num of contractors providing smoking cessation consultations
      NUM_SCS_CNSLT= sum(SCS_CNSLT>0, na.rm=TRUE),
      # # total cost of  smoking cessation consultations
      SCS_CNSLT= sum(SCS_CNSLT, na.rm=TRUE),
      # # total cost of  smoking cessation product charges
      SCS_NRTPROD_CHARGE = sum(SCS_NRTPROD_CHARGE, na.rm=TRUE),
      # # total cost of  smoking cessation product costs
      SCS_NRTPROD_COST= sum(SCS_NRTPROD_COST, na.rm=TRUE),
      # # total cost of  smoking cessation set up
      SCS_SETUP= sum(SCS_SETUP, na.rm=TRUE),
      # # num of contractors  serious shortage protocols
      NUM_SSP_FEES= sum(SSP_FEES>0, na.rm=TRUE),
      # # total cost of  serious shortage protocols
      SSP_FEES= sum(SSP_FEES, na.rm=TRUE),
      # # num of contractors receiving GP referral path fees
      NUM_GPREFPATH_FEES= sum(GPREFPATH_FEES>0, na.rm=TRUE),
      # # total cost of GP referral path fees
      GPREFPATH_FEES= sum(GPREFPATH_FEES, na.rm=TRUE),
      # # num of contractors receiving covid test reg fees
      NUM_CVDTESTREG_FEES= sum(CVDTESTREG_FEES>0, na.rm=TRUE),
      # # total cost of covid test reg fees
      CVDTESTREG_FEES= sum(CVDTESTREG_FEES, na.rm=TRUE),
      # # num of contractors receiving covid test set up fees
      NUM_CVDTESTSET_FEES= sum(CVDTESTSET_FEES>0, na.rm=TRUE),
      # # total cost of covid test set upfees
      CVDTESTSET_FEES= sum(CVDTESTSET_FEES, na.rm=TRUE),
      # # num of contractors receiving covid test kits
      NUM_CVDTEST_KITS= sum(CVDTEST_KITS>0, na.rm=TRUE),
      # # total cost of covid test kits
      CVDTEST_KITS= sum(CVDTEST_KITS, na.rm=TRUE),
      # # num of contractors receiving covid 19 costs
      NUM_CVD_19_COSTS= sum(CVD_19_COSTS>0, na.rm=TRUE),
      # # total cost of covid 19 costs
      CVD_19_COSTS= sum(CVD_19_COSTS, na.rm=TRUE),
      # # num of contractors receiving covid 19 vaccine
      NUM_CVD_19_VACCINE = sum(CVD_19_VACCINE>0, na.rm=TRUE),
      # # total cost of covid 19 vaccine
      CVD_19_VACCINE = sum(CVD_19_VACCINE, na.rm=TRUE),
      # # num of contractors receiving covid 19 premises refrigeration
      NUM_CVD_PREM_REFRID= sum(CVD_PREM_REFRID>0, na.rm=TRUE),
      # # total cost of covid 19 premises refrigeration
      CVD_PREM_REFRID= sum(CVD_PREM_REFRID, na.rm=TRUE),
      # # num of contractors receiving discharge meds
      NUM_DISCHARGE_MEDS= sum(DISCHARGE_MEDS>0, na.rm=TRUE),
      # # total cost of covid 19 discharge meds
      DISCHARGE_MEDS= sum(DISCHARGE_MEDS, na.rm=TRUE),
      # # num of contractors receiving ppe claims
      NUM_PPE_CLAIMS= sum(PPE_CLAIMS>0, na.rm=TRUE),
      # # total cost of covid 19 PPE claims
      PPE_CLAIMS= sum(PPE_CLAIMS, na.rm=TRUE),
      # # num of contractors providing tier 1 contraceptive consultations
      num_t1c_cnslt= sum(T1C_CONSULT>0, na.rm=TRUE),
      # # total cost of tier 1 contraceptive set up
      t1c_set_up = sum(T1C_SET_UP,na.rm=TRUE),
      # # total cost of tier 1 contraceptive product
      t1c_prod_cost = sum(T1C_PROD_COST,na.rm=TRUE),
      # # total cost of tier 1 contraceptive consults
      t1c_consult = sum(T1C_CONSULT,na.rm=TRUE),
      # # num of contractors receiving pharmacy first payments
      num_pfcp_payment= sum(PFCP_PAYMENT>0, na.rm=TRUE),
      # # total cost of pharmacy first opt in
      pfcp_optin = sum(PFCP_OPTIN,na.rm=TRUE),
      # # total cost of pharmacy first fees
      pfcp_fees = sum(PFCP_FEES,na.rm=TRUE),
      # # total cost of pharmacy first opt payment
      pfcp_payment = sum(PFCP_PAYMENT,na.rm=TRUE),
      # # total cost of pharmacy first months
      pfcp_months = sum(PFCP_MONTHS,na.rm=TRUE),
      # # total cost of pharmacy first VAT
      pfcp_vat = sum(PFCP_VAT,na.rm=TRUE),
      # # total cost of pharmacy first deductions
      pfcp_umsmideduct = sum(PFCP_UMSMIDEDUCT,na.rm=TRUE),
      # # total cost of pharmacy first remuneration
      pfcp_umsmiremuneration = sum(PFCP_UMSMIREMUNERATION,na.rm=TRUE),
      # # total cost of pharmacy first reimbursent
      pfcp_umsmireimbursement = sum(PFCP_UMSMIREIMBURSEMENT,na.rm=TRUE)
      ) %>%

    dplyr::arrange(YEAR_MONTH, APPLIANCE_DISPENSER_HIST) %>%
    collect()



  return(fact)

}
#'Extract national level data for GPhS
#'
#'Easy helper for 'national_extract' returns national level data from GPhs final fact table
#'
#'
#' @param con the connection object
#' @param table the name of the GPhS fact table built in your schema
#'
#' @import nhsbsaR
#' @import dplyr
#' @import stringr
#'
#' @export
#'
#' @examples
#' national_extract <- national_extract(
# con = con,
# schema = "SCHEMA",
# table = "GPS_MONTH_202309"
#')



national_extract <- function(con,
                             schema,
                             table) {

  fact <- dplyr::tbl(con,
                     from = table)  %>%
    collect() %>%
    # create any helper cols
    dplyr::mutate(
      # set dates in year 9999 to NA
      CLOSE_DATE_HIST = case_when(
        substr(CLOSE_DATE_HIST, 1, 4) == "9999" ~ as.character(NA),
        TRUE ~ as.character(CLOSE_DATE_HIST)
      ),
      START_DATE_HIST = case_when(
        substr(START_DATE_HIST, 1, 4) == "9999" ~ as.character(NA),
        TRUE ~ as.character(START_DATE_HIST)
      ),
      # create first and last day on fy helper cols
      last_day = as.Date(paste0(substr(FINANCIAL_YEAR,6,9),"0331"), format = "%Y%m%d"),
      first_day = as.Date(paste0(substr(FINANCIAL_YEAR,1,4),"0401"), format = "%Y%m%d"),
      # convert contractor start and end dates to datetime objects
      across(contains("DATE_HIST"), ~ as.Date(.x, format = "%d/%m/%Y")),
      # all AUR fees col
      AUR_ALL = AUR_HOME + AUR_PREM,
      # all AUF num col
      NUM_AUR_ALL = NUM_AUR_HOME + NUM_AUR_PREM
    ) %>%
    dplyr::group_by(FINANCIAL_YEAR,APPLIANCE_DISPENSER_HIST) %>%
    # summarise cols to reported measures
    dplyr::summarise(
      # # number of community pharmacies
      num_contractors = n(),
      # # total number of items dispensed
      items_total = sum(ITEMS, na.rm=TRUE),
      # # total nic(costs) of items dispensed
      costs_total = sum(NIC, na.rm=TRUE),
      # # total number of dispensing fees
      prof_fees_total_pharm = sum(NUM_PROF_FEES, na.rm=TRUE),
      # # value of dispensing fees
      prof_fees_pharm = sum(PROF_FEES, na.rm=TRUE),
      # # mean average monthly items
      avg_monthly_items_pharm = median(AVG_MONTHLY_ITEMS, na.rm=TRUE),
      # # num of contractors for dispensing bands
      band_items_0_2000 = sum(MONTHLY_DISP_VOL_BAND == "0 - 2000", na.rm=TRUE),
      band_items_2001_4000 = sum(MONTHLY_DISP_VOL_BAND == "2001 - 4000", na.rm=TRUE),
      band_items_4001_6000 = sum(MONTHLY_DISP_VOL_BAND == "4001 - 6000", na.rm=TRUE),
      band_items_6001_8000 = sum(MONTHLY_DISP_VOL_BAND == "6001 - 8000", na.rm=TRUE),
      band_items_8001_10000 = sum(MONTHLY_DISP_VOL_BAND == "8001 - 10000", na.rm=TRUE),
      band_items_10000 = sum(MONTHLY_DISP_VOL_BAND == "10000+", na.rm=TRUE),
      # items dispensed by contractors in dispensing bands
      vol_items_0_2000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "0 - 2000"], na.rm=TRUE),
      vol_items_2001_4000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "2001 - 4000"], na.rm=TRUE),
      vol_items_4001_6000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "4001 - 6000"], na.rm=TRUE),
      vol_items_6001_8000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "6001 - 8000"], na.rm=TRUE),
      vol_items_8001_10000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "8001 - 10000"], na.rm=TRUE),
      vol_items_10000 = sum(ITEMS[MONTHLY_DISP_VOL_BAND == "10000+"], na.rm=TRUE),
      # # number of independent pharmacies
      num_ind_pharm = sum(CONTRACTOR_TYPE == "Independent", na.rm=TRUE),
      # # number of multiples
      num_multi_pharm = sum(CONTRACTOR_TYPE == "Multiple contractor", na.rm=TRUE),
      # # number of distance selling
      num_dist_selling_pharm = sum(DIST_SELLING_DISPENSER_HIST == "Y", na.rm=TRUE),
      # # number that opened in the year
      num_open_pharm = sum(START_DATE_HIST >= first_day, na.rm=TRUE),
      # # number that closed in the year
      num_close_pharm = sum(CLOSE_DATE_HIST <= last_day, na.rm=TRUE),
      # # number of lps contracts
      num_lps_pharm = sum(LPS_DISPENSER_HIST == "Y", na.rm=TRUE),
      # # number contractors that provided MURs
      num_mur_pharm = sum(NUM_MUR > 0, na.rm=TRUE),
      # # total number of MURs
      num_mur_total = sum(NUM_MUR, na.rm=TRUE),
      # # total of MUR fees
      num_mur_fee_total = sum(MUR_FEES, na.rm=TRUE),
      # # number of contractors that provided NMS
      num_nms_pharm = sum(NUM_NMS > 0, na.rm=TRUE),
      # # total number of NMS
      num_nms_total = sum(NUM_NMS, na.rm=TRUE),
      # # total of NMS fees
      num_nms_fee_total = sum(NMS_FEES, na.rm=TRUE),
      # # num contractors providing AUR home
      num_aur_home_pharm = sum(NUM_AUR_HOME > 0, na.rm=TRUE),
      # # num contractors providing AUR prem
      num_aur_prem_pharm = sum(NUM_AUR_PREM > 0, na.rm=TRUE),
      # # num of contractors providing any AUR
      num_aur_all_pharm = sum(NUM_AUR_ALL > 0, na.rm=TRUE),
      # # total AUR home
      num_aur_home_total_pharm = sum(NUM_AUR_HOME, na.rm=TRUE),
      # # total AUR prem
      num_aur_prem_total_pharm = sum(NUM_AUR_PREM, na.rm=TRUE),
      # # total AUR all
      num_aur_all_total_pharm = sum(NUM_AUR_ALL, na.rm=TRUE),
      # # total AUR home fee
      num_aur_home_total_fee_pharm = sum(AUR_HOME, na.rm=TRUE),
      # # total AUR prem fee
      num_aur_prem_total_fee_pharm = sum(AUR_PREM, na.rm=TRUE),
      # # total AUR all
      num_aur_all_total_fee_pharm = sum(AUR_ALL, na.rm=TRUE),
      # # number of contractors doing SAC
      num_stoma_pharm = sum(NUM_CUSTOM_STOMA_FEES > 0, na.rm=TRUE),
      # # total number of SAC
      num_stoma_total_pharm = sum(NUM_CUSTOM_STOMA_FEES, na.rm=TRUE),
      # # total number of SAC fee
      num_stoma_total_fee_pharm = sum(CUSTOM_STOMA_FEES, na.rm=TRUE),
      # # number of contractors providing home delivery
      num_home_del = sum(NUM_HOME_DEL > 0, na.rm=TRUE),
      # # total home delivery fees
      num_home_del_total = sum(NUM_HOME_DEL, na.rm=TRUE),
      # #total home delivery cost
      home_del_cost_total = sum(HOME_DEL_COST, na.rm=TRUE),
      # # number of contractors providing flu
      num_flu_pharm = sum(FLU_ITEMS > 0, na.rm=TRUE),
      # # total flu vaccines
      flu_items_total = sum(FLU_ITEMS, na.rm=TRUE),
      # # total flu costs
      flu_cost_total = sum(FLU_COST, na.rm=TRUE),
      # # total flu fees
      flu_fees_total = sum(FLU_FEES, na.rm=TRUE),
      # # number of contractors using EPS
      num_eps_pharm = sum(EPS_ITEMS > 0, na.rm=TRUE),
      # # total EPS items
      eps_items_total_pharm = sum(EPS_ITEMS, na.rm=TRUE),
      # # num of contractors providing cd_fee
      num_cd_fee = sum(CD_FEE > 0, na.rm=TRUE),
      # # total cost of  cd_fee
      cd_fee = sum(CD_FEE, na.rm=TRUE),
      # # num of contractors providing cd_sched2_fee
      num_cd_sched2_fee = sum(CD_SCHED2_FEE > 0, na.rm=TRUE),
      # # total cost of  cd_sched2_fee
      cd_sched2_fee = sum(CD_SCHED2_FEE, na.rm=TRUE),
      # # num of contractors providing cd_sched3_fee
      num_cd_sched3_fee = sum(CD_SCHED3_FEE > 0, na.rm=TRUE),
      # # total cost of  cd_sched3_fee
      cd_sched3_fee = sum(CD_SCHED3_FEE, na.rm=TRUE),
      # # num of contractors providing add_fee_2a
      num_add_fee_2a = sum(ADD_FEE_2A > 0, na.rm=TRUE),
      # # total cost of  add_fee_2a
      add_fee_2a= sum (ADD_FEE_2A, na.rm=TRUE),
      # # num of contractors providing add_fee_mf
      num_add_fee_mf_pharm = sum(ADD_FEE_MF > 0, na.rm=TRUE),
      # # total cost of  add_fee_mf
      add_fee_mf_pharm = sum(ADD_FEE_MF, na.rm=TRUE),
      # # num of contractors providing expensive_fee
      num_expensive_fees_pharm= sum(EXPENSIVE_FEE > 0, na.rm=TRUE),
      # # total number of  no_expensive_fees
      no_expensive_fees_pharm = sum(NO_EXPENSIVE_FEES, na.rm=TRUE),
      # # total cost of  expensive_fee
      expensive_fee_pharm = sum(EXPENSIVE_FEE, na.rm=TRUE),
      # # num of contractors providing oope_item_count
      num_oope_item_count_pharm = sum(OOPE_ITEM_COUNT > 0, na.rm=TRUE),
      # # total number of  oope_item_count
      oope_item_count_pharm = sum(OOPE_ITEM_COUNT, na.rm=TRUE),
      # # total cost of  oope_val
      oope_val_pharm = sum(OOPE_VAL, na.rm=TRUE),
      # # num of contractors providing ums_drugs
      num_ums_drugs_pharm = sum(UMS_DRUGS > 0, na.rm=TRUE),
      # # total fees cost of  ums_fees
      ums_fees_pharm = sum(UMS_FEES, na.rm=TRUE),
      # # total cost of  ums_drugs
      ums_drugs_pharm = sum(UMS_DRUGS, na.rm=TRUE),
      # # num of contractors providing cpcs_drugs
      num_cpcs_drugs_pharm = sum(CPCS_FEES > 0, na.rm=TRUE),
      # # total fees cost of  cpcs_fees
      cpcs_fees_pharm = sum(CPCS_FEES, na.rm=TRUE),
      # # total cost of  cpcs_drugs
      cpcs_drugs_pharm = sum(CPCS_DRUGS, na.rm=TRUE),
      # # num of contractors providing hep_c_service
      num_hep_c_service_pharm = sum(HEP_C_SERVICE > 0, na.rm=TRUE),
      # # total cost of  hep_c_kit
      hep_c_kit_pharm = sum(HEP_C_KIT, na.rm=TRUE),
      # # total cost of  hep_c_service
      hep_c_service_pharm = sum(HEP_C_SERVICE, na.rm=TRUE),
      # # num of contractors providing hypertension incentives
      NUM_HYPTEN_INC= sum(HYPTEN_INC>0, na.rm=TRUE),
      # # total cost of  hypertension check fees
      HYPTENCHECK_FEES= sum(HYPTENCHECK_FEES, na.rm=TRUE),
      # # total cost of  hypertension set up fees
      HYPTENSET_FEES= sum(HYPTENSET_FEES, na.rm=TRUE),
      # # total cost of  hypertension incentives
      HYPTEN_INC= sum(HYPTEN_INC, na.rm=TRUE),
      # # num of contractors providing smoking cessation consultations
      NUM_SCS_CNSLT= sum(SCS_CNSLT>0, na.rm=TRUE),
      # # total cost of  smoking cessation consultations
      SCS_CNSLT= sum(SCS_CNSLT, na.rm=TRUE),
      # # total cost of  smoking cessation product charges
      SCS_NRTPROD_CHARGE = sum(SCS_NRTPROD_CHARGE, na.rm=TRUE),
      # # total cost of  smoking cessation product costs
      SCS_NRTPROD_COST= sum(SCS_NRTPROD_COST, na.rm=TRUE),
      # # total cost of  smoking cessation set up
      SCS_SETUP= sum(SCS_SETUP, na.rm=TRUE),
      # # num of contractors  serious shortage protocols
      NUM_SSP_FEES= sum(SSP_FEES>0, na.rm=TRUE),
      # # total cost of  serious shortage protocols
      SSP_FEES= sum(SSP_FEES, na.rm=TRUE),
      # # num of contractors receiving GP referral path fees
      NUM_GPREFPATH_FEES= sum(GPREFPATH_FEES>0, na.rm=TRUE),
      # # total cost of GP referral path fees
      GPREFPATH_FEES= sum(GPREFPATH_FEES, na.rm=TRUE),
      # # num of contractors receiving covid test reg fees
      NUM_CVDTESTREG_FEES= sum(CVDTESTREG_FEES>0, na.rm=TRUE),
      # # total cost of covid test reg fees
      CVDTESTREG_FEES= sum(CVDTESTREG_FEES, na.rm=TRUE),
      # # num of contractors receiving covid test set up fees
      NUM_CVDTESTSET_FEES= sum(CVDTESTSET_FEES>0, na.rm=TRUE),
      # # total cost of covid test set upfees
      CVDTESTSET_FEES= sum(CVDTESTSET_FEES, na.rm=TRUE),
      # # num of contractors receiving covid test kits
      NUM_CVDTEST_KITS= sum(CVDTEST_KITS>0, na.rm=TRUE),
      # # total cost of covid test kits
      CVDTEST_KITS= sum(CVDTEST_KITS, na.rm=TRUE),
      # # num of contractors receiving covid 19 costs
      NUM_CVD_19_COSTS= sum(CVD_19_COSTS>0, na.rm=TRUE),
      # # total cost of covid 19 costs
      CVD_19_COSTS= sum(CVD_19_COSTS, na.rm=TRUE),
      # # num of contractors receiving covid 19 vaccine
      NUM_CVD_19_VACCINE = sum(CVD_19_VACCINE>0, na.rm=TRUE),
      # # total cost of covid 19 vaccine
      CVD_19_VACCINE = sum(CVD_19_VACCINE, na.rm=TRUE),
      # # num of contractors receiving covid 19 premises refrigeration
      NUM_CVD_PREM_REFRID= sum(CVD_PREM_REFRID>0, na.rm=TRUE),
      # # total cost of covid 19 premises refrigeration
      CVD_PREM_REFRID= sum(CVD_PREM_REFRID, na.rm=TRUE),
      # # num of contractors receiving discharge meds
      NUM_DISCHARGE_MEDS= sum(DISCHARGE_MEDS>0, na.rm=TRUE),
      # # total cost of covid 19 discharge meds
      DISCHARGE_MEDS= sum(DISCHARGE_MEDS, na.rm=TRUE),
      # # num of contractors receiving ppe claims
      NUM_PPE_CLAIMS= sum(PPE_CLAIMS>0, na.rm=TRUE),
      # # total cost of covid 19 PPE claims
      PPE_CLAIMS= sum(PPE_CLAIMS, na.rm=TRUE),
      # # num of contractors receiving tier 1 contraceptive consults
      num_t1c_cnslt= sum(T1C_CONSULT>0, na.rm=TRUE),
      # # total cost of tier 1 contraceptive set up
      t1c_set_up = sum(T1C_SET_UP,na.rm=TRUE),
      # # total cost of tier 1 contraceptive product
      t1c_prod_cost = sum(T1C_PROD_COST,na.rm=TRUE),
      # # total cost of tier 1 contraceptive consults
      t1c_consult = sum(T1C_CONSULT,na.rm=TRUE),
      # # num of contractors receiving pharmacy first payments
      num_pfcp_payment= sum(PFCP_PAYMENT>0, na.rm=TRUE),
      # # total cost of pharmacy first opt in
      pfcp_optin = sum(PFCP_OPTIN,na.rm=TRUE),
      # # total cost of pharmacy first fees
      pfcp_fees = sum(PFCP_FEES,na.rm=TRUE),
      # # total cost of pharmacy first opt payment
      pfcp_payment = sum(PFCP_PAYMENT,na.rm=TRUE),
      # # total cost of pharmacy first months
      pfcp_months = sum(PFCP_MONTHS,na.rm=TRUE),
      # # total cost of pharmacy first VAT
      pfcp_vat = sum(PFCP_VAT,na.rm=TRUE),
      # # total cost of pharmacy first deductions
      pfcp_umsmideduct = sum(PFCP_UMSMIDEDUCT,na.rm=TRUE),
      # # total cost of pharmacy first remuneration
      pfcp_umsmiremuneration = sum(PFCP_UMSMIREMUNERATION,na.rm=TRUE),
      # # total cost of pharmacy first reimbursent
      pfcp_umsmireimbursement = sum(PFCP_UMSMIREIMBURSEMENT,na.rm=TRUE)

    ) %>%
    dplyr::arrange(FINANCIAL_YEAR, APPLIANCE_DISPENSER_HIST) %>%
    collect()



  return(fact)

}
# Table functions
#' General Pharmaceutical Services table 1 aggregations function
#'
#' This function takes the output from the GPhS 'national_extract' function
#' and aggregates it into national number of pharmacies and appliance contractors
#'
#' @param national_extract The data resulting from using national_extract()
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_1 <- table_1(national_extract)

table_1 <- function(national_extract){

  table_1 <- national_extract %>%
    dplyr::select(FINANCIAL_YEAR, APPLIANCE_DISPENSER_HIST, num_contractors) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    dplyr::summarise(`Community pharmacies` = sum(num_contractors[APPLIANCE_DISPENSER_HIST=="N"]),
                     `Appliance contractors` = sum(num_contractors[APPLIANCE_DISPENSER_HIST=="Y"]),
                     `Community pharmacies and appliance contractors` = sum(num_contractors)) %>%
    dplyr::ungroup()

  return(table_1)

}
#' General Pharmaceutical Services table 2 aggregations function
#'
#' This function takes the output from the GPhS 'national_extract' function
#' and aggregates it into numbers of national active community pharmacies
#'
#' @param national_extract The data resulting from using national_extract()
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_2 <- table_2(national_extract)

table_2 <- function(national_extract){

  table_2 <- national_extract %>%
    #filter for community pharmacies only
    dplyr::filter(APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR, num_contractors,
                  num_open_pharm, num_close_pharm, num_lps_pharm,
                  num_ind_pharm, num_multi_pharm, num_dist_selling_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    #get totals for columns by Financial Year
    dplyr::summarise(`Community pharmacies` = sum(num_contractors),
                     `Pharmacies opened` = sum(num_open_pharm),
                     `Pharmacies closed` = sum(num_close_pharm),
                     `LPS contractors` = sum(num_lps_pharm),
                     `Independent contractors` = sum(num_ind_pharm),
                     `Multiple contractors` = sum(num_multi_pharm),
                     `Distance selling pharmacies` = sum(num_dist_selling_pharm)) %>%
    ungroup()

}

#' General Pharmaceutical Services table 3 aggregations function
#'
#' This function takes the output from the GPhS 'national_extract' function
#' and aggregates it into national community pharmacy essential and enhanced services
#'
#' @param national_extract The data resulting from using national_extract()
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_3 <- table_3(national_extract)

table_3 <- function(national_extract){

  table_3 <- national_extract %>%
    #filter for community pharmacies only
    dplyr::filter(APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR, num_contractors, items_total, costs_total,
                  avg_monthly_items_pharm, num_eps_pharm, eps_items_total_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    #get totals for columns by Financial Year
    dplyr::summarise(`Community pharmacies` = sum(num_contractors),
                     `Total prescription items dispensed` = sum(items_total),
                     `Total cost of prescription items dispensed (GBP)` = sum(costs_total),
                     `Average monthly items per pharmacy` = avg_monthly_items_pharm,
                     `Pharmacies dispensing via EPS` = sum(num_eps_pharm),
                     `Total items dispensed via EPS` = sum(eps_items_total_pharm),
                     `Percentage of items dispensed via EPS (%)` =
                       ((`Total items dispensed via EPS` / `Total prescription items dispensed`) * 100)
    ) %>%
    ungroup()

}
#' General Pharmaceutical Services table  4 aggregations function
#'
#' This function takes the output from the GPhS 'national_extract' function
#' and aggregates it into number of community pharmacies within dispensing bands
#' and volume of items in dispensing bands
#'
#' @param national_extract The data resulting from using national_extract()
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_4 <- table_4(national_extract)

table_4 <- function(national_extract){
  table_4a <- national_extract %>%
    #filter for community pharmacies only
    dplyr::filter(APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR,
                  band_items_0_2000, band_items_2001_4000, band_items_4001_6000,
                  band_items_6001_8000, band_items_8001_10000, band_items_10000) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    #get totals for columns by Financial Year
    dplyr::summarise(
      `0 - 2000` = sum(band_items_0_2000),
      `2001 - 4000` = sum(band_items_2001_4000),
      `4001 - 6000` = sum(band_items_4001_6000),
      `6001 - 8000` = sum(band_items_6001_8000),
      `8001 - 10000` = sum(band_items_8001_10000),
      `10000+` = sum(band_items_10000)
    ) %>%
    ungroup() %>%
    gather(
      "Dispensing volume band", "Number of pharmacies", -`Financial Year`
    )
  table_4b <- national_extract %>%
    #filter for community pharmacies only
    dplyr::filter(APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR,
                  vol_items_0_2000, vol_items_2001_4000, vol_items_4001_6000,
                  vol_items_6001_8000, vol_items_8001_10000, vol_items_10000) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    dplyr::summarise(
      `0 - 2000` = sum(vol_items_0_2000),
      `2001 - 4000` = sum(vol_items_2001_4000),
      `4001 - 6000` = sum(vol_items_4001_6000),
      `6001 - 8000` = sum(vol_items_6001_8000),
      `8001 - 10000` = sum(vol_items_8001_10000),
      `10000+` = sum(vol_items_10000)
    ) %>%
    ungroup() %>%
    gather(
      "Dispensing volume band", "Number of items", -`Financial Year`
    )
  table_4 <- table_4a %>%
    left_join(table_4b) %>%
    arrange(
      `Financial Year`,
      match(
        `Dispensing volume band`,
        c(
          "0 - 2000",
          "2001 - 4000",
          "4001 - 6000",
          "6001 - 8000",
          "8001 - 10000",
          "10000+"
        )
      ),
    )
  return(table_4)
}
#' General Pharmaceutical Services table 5 aggregations function
#'
#' This function takes the output from the GPhS 'national_extract' function
#' and aggregates it into essential services fees and expenses of community
#' pharmacies at a national level
#'
#' @param national_extract The data resulting from using national_extract()
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_5 <- table_5(national_extract)

table_5 <- function(national_extract){

  table_5 <- national_extract %>%
    #filter for community pharmacies only
    dplyr::filter(APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR, num_contractors, prof_fees_total_pharm,
                  prof_fees_pharm, costs_total, num_cd_fee, cd_fee, num_cd_sched2_fee, cd_sched2_fee,
                  num_cd_sched3_fee, cd_sched3_fee, num_add_fee_2a, add_fee_2a,
                  num_add_fee_mf_pharm, add_fee_mf_pharm, num_expensive_fees_pharm,
                  no_expensive_fees_pharm, expensive_fee_pharm, num_oope_item_count_pharm,
                  oope_item_count_pharm, oope_val_pharm, NUM_DISCHARGE_MEDS,
                  DISCHARGE_MEDS) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    #get totals for columns by Financial Year
    dplyr::summarise(`Community pharmacies` = sum(num_contractors),
                     `Total number of dispensing fees received` = sum(prof_fees_total_pharm),
                     `Total value of dispensing fees received (GBP)` = sum(prof_fees_pharm),
                     `Average number of fees per pharmacy` = (`Total number of dispensing fees received`/`Community pharmacies`),
                     `Average cost per fee (GBP)` = (round(costs_total/`Total number of dispensing fees received`,2)),
                     `Pharmacies receiving Methodone fees` = sum(num_cd_fee),
                     `Total cost of Methadone fees (GBP)` = sum(cd_fee),
                     `Pharmacies receiving Schedule 2 CD fees` = sum(num_cd_sched2_fee),
                     `Total cost of Schedule 2 CD fees (GBP)` = sum(cd_sched2_fee),
                     `Pharmacies receiving Schedule 3 CD fees` = sum(num_cd_sched3_fee),
                     `Total cost of Schedule 3 CD fees (GBP)` = sum(cd_sched3_fee),
                     `Pharmacies receiving extemporaneous preparation fees` = sum(num_add_fee_2a),
                     `Total cost of extemporaneous preparation fees (GBP)` = sum(add_fee_2a),
                     `Pharmacies receiving measure and fit fees` = sum(num_add_fee_mf_pharm),
                     `Total cost of measure and fit fees (GBP)` = sum(add_fee_mf_pharm),
                     `Pharmacies receiving expensive item fees` = sum(num_expensive_fees_pharm),
                     `Total number of expensive item fees` = sum(no_expensive_fees_pharm),
                     `Total cost of expensive item fees (GBP)` = sum(expensive_fee_pharm),
                     `Pharmacies receiving OOPE` = sum(num_oope_item_count_pharm),
                     `Total number of OOPE` = sum(oope_item_count_pharm),
                     `Total cost of OOPE (GBP)` = sum(oope_val_pharm),
                     `Pharmacies providing DMS` = sum(NUM_DISCHARGE_MEDS),
                     `Total cost of DMS (GBP)` = sum(DISCHARGE_MEDS)
    ) %>%
    ungroup()

}
#' Function for table_6
#'
#' Function to create table from GPhS summary tables, using output from
#' 'national_extract' function, to get advanced services and fees for
#' community pharmacies, including New Medicine Service (NMS), the discontinued
#' Medicines Use Review (MUR), seasonal flu vaccine, Stoma Customisation service
#' (SAC), Hepatitis C testing, hypertension checks and smoking cessation. Tier 1
#' contraception and Pharmacy first
#'
#' @param national_extract the output of the national_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_6 <- table_6(national_extract)

table_6 <- function(national_extract){

  table_6 <- national_extract %>%
    #filter for community pharmacies only
    dplyr::filter(APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR, num_contractors, num_nms_pharm, num_nms_total,
                  num_nms_fee_total, num_flu_pharm_csv, flu_items_total_csv, flu_cost_total_csv,
                  flu_fees_total_csv, num_stoma_pharm, num_stoma_total_pharm,
                  num_stoma_total_fee_pharm, num_hep_c_service_pharm, hep_c_service_pharm,
                  hep_c_kit_pharm, NUM_HYPTEN_INC, HYPTENSET_FEES, HYPTENCHECK_FEES,
                  HYPTEN_INC, num_mur_pharm, num_mur_total, num_mur_fee_total,
                  NUM_SCS_CNSLT, SCS_CNSLT, SCS_NRTPROD_COST, SCS_SETUP,num_t1c_cnslt,t1c_set_up,t1c_prod_cost,
                  t1c_consult,num_pfcp_payment, pfcp_optin, pfcp_fees, pfcp_payment,pfcp_months,
                  pfcp_vat,pfcp_umsmideduct, pfcp_umsmiremuneration, pfcp_umsmireimbursement


    ) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    #get totals for columns by Financial Year
    dplyr::summarise(`Community Pharmacies` = sum(num_contractors),
                     `Pharmacies providing NMS` = sum(num_nms_pharm),
                     `Total amount of NMS provided` = sum(num_nms_total),
                     `Total cost of NMS provided (GBP)` = sum(num_nms_fee_total),
                     `Pharmacies providing Flu vaccine service` =
                       sum(num_flu_pharm_csv),
                     `Total amount of Flu vaccine service provided` =
                       sum(flu_items_total_csv),
                     `Total cost of Flu vaccine service provided (GBP)` =
                       sum(flu_cost_total_csv),
                     `Total amount of fees received for Flu vaccine service (GBP)` =
                       sum(flu_fees_total_csv),
                      `Pharmacies providing Hypertension service` =
                       sum(NUM_HYPTEN_INC),
                     `Total cost of set up of Hypertension service (GBP)` =
                       sum(HYPTENSET_FEES),
                     `Total cost of Hypertension service fees (GBP)` =
                       sum(HYPTENCHECK_FEES),
                     `Total cost of Hypertension service incentives (GBP)` =
                       sum(HYPTEN_INC),
                     `Pharmacies providing SCS` =
                       sum(NUM_SCS_CNSLT),
                     `Total cost of SCS set up fees (GBP)` =
                       sum(SCS_SETUP),
                     `Total cost of SCS consultations (GBP)` =
                       sum(SCS_CNSLT),
                     `Total cost of SCS products (GBP)` =
                       sum(SCS_NRTPROD_COST),
                     `Pharmacies providing PCSs` =
                       sum(num_t1c_cnslt),
                     `Total cost of PCS set up fees (GBP)` =
                       sum(t1c_set_up),
                     `Total cost of PCS consultations (GBP)` =
                       sum(t1c_consult),
                     `Total cost of PCS products (GBP)` =
                       sum(t1c_prod_cost),
                     `Pharmacies providing PFS` =
                       sum(num_pfcp_payment),
                     `Total cost of  PFS set up fees (GBP)` =
                       sum(pfcp_optin),
                     `Total cost of  PFS fees (GBP)` =
                       sum(pfcp_fees),
                     `Total cost of  PFS payment (GBP)` =
                       sum(pfcp_payment),
                     `Total cost of  PFS VAT (GBP)` =
                       sum(pfcp_vat),
                     `Total cost of  PFS UMS Deductions (GBP)` =
                       sum(pfcp_umsmideduct),
                     `Total cost of  PFS UMS Remuneration (GBP)` =
                       sum(pfcp_umsmiremuneration),
                     `Total cost of  PFS UMS Reimbursement (GBP)` =
                       sum(pfcp_umsmireimbursement),
                     `Pharmacies providing Hep C testing service` =
                       sum(num_hep_c_service_pharm),
                     `Total cost of provision of Hep C testing service (GBP)` =
                       sum(hep_c_service_pharm),
                     `Total cost of provision of Hep C Test Kit reimbursement (GBP)` =
                       sum(hep_c_kit_pharm),
                     `Pharmacies providing MURs` =
                       sum(num_mur_pharm),
                     `Total amount of MURs provided` =
                       sum(num_mur_total),
                     `Total cost of MURs (GBP)` =
                       sum(num_mur_fee_total)) %>%
    ungroup()

}
#' Function for table_7
#'
#' Function to create table from GPhS summary tables, using output from
#' 'national_extract' function, to get appliance use review advanced service for
#'  community pharmacies
#'
#' @param national_extract the output of the national_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_7 <- table_7(national_extract)

table_7 <- function(national_extract){

  table_7 <- national_extract %>%
    #filter for community pharmacies only
    dplyr::filter(APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR, num_contractors, num_aur_home_pharm,
                  num_aur_home_total_pharm, num_aur_home_total_fee_pharm,
                  num_aur_prem_pharm, num_aur_prem_total_pharm,
                  num_aur_prem_total_fee_pharm, num_aur_all_pharm,
                  num_aur_all_total_pharm, num_aur_all_total_fee_pharm,
                  num_stoma_pharm, num_stoma_total_pharm, num_stoma_total_fee_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    #get totals for columns by Financial Year
    dplyr::summarise(`Community pharmacies` = sum(num_contractors),
                     `Pharmacies providing home AURs` = sum(num_aur_home_pharm),
                     `Total amount of home AURs` = sum(num_aur_home_total_pharm),
                     `Total cost of home AURs provided (GBP)` = sum(num_aur_home_total_fee_pharm),
                     `Pharmacies providing premises AURs` = sum(num_aur_prem_pharm),
                     `Total amount of premises AURs` = sum(num_aur_prem_total_pharm),
                     `Total cost of premises AURs (GBP)` = sum(num_aur_prem_total_fee_pharm),
                     `Pharmacies providing any AURs` = sum(num_aur_all_pharm),
                     `Total amount of all AURs` = sum(num_aur_all_total_pharm),
                     `Total cost of all AURs (GBP)` = sum(num_aur_all_total_fee_pharm),
                     `Pharmacies providing SAC` =
                       sum(num_stoma_pharm),
                     `Total amount of SAC` =
                       sum(num_stoma_total_pharm),
                     `Total cost of SAC (GBP)` =
                       sum(num_stoma_total_fee_pharm)) %>%
    ungroup()

}
#' Function for table_8
#'
#' Function to create table from GPhS summary tables, using output from
#' 'national_extract' function, to get fees and services relating to the
#' Community Pharmacist Consultation Service (CPCS) advanced service for
#' community pharmacies, including NHS urgent medicine supply service (NUMSAS)
#' and GP referral pathway to CPCS
#'
#' @param national_extract the output of the national_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_8 <- table_8(national_extract)

table_8 <- function(national_extract){

  table_8 <- national_extract %>%
    #filter for community pharmacies only
    dplyr::filter(APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR, num_contractors, num_cpcs_drugs_pharm,
                  cpcs_fees_pharm, cpcs_drugs_pharm, num_ums_drugs_pharm,
                  ums_fees_pharm, ums_drugs_pharm, NUM_GPREFPATH_FEES, GPREFPATH_FEES) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    #get totals for columns by Financial Year
    dplyr::summarise(`Community pharmacies` = sum(num_contractors),
                     `Pharmacies providing CPCS` =
                       sum(num_cpcs_drugs_pharm),
                     `Total amount of fees received for CPCS (GBP)` =
                       sum(cpcs_fees_pharm),
                     `Total cost of CPCS drugs (GBP)` = sum(cpcs_drugs_pharm),
                     `Pharmacies claiming engagement fee` = sum(NUM_GPREFPATH_FEES),
                     `Total cost of engagement fees (GBP)` = sum(GPREFPATH_FEES),
                     `Pharmacies providing NUMSAS` = sum(num_ums_drugs_pharm),
                     `Total amount of fees received for NUMSAS (GBP)` = sum(ums_fees_pharm),
                     `Total cost of NUMSAS drugs (GBP)` = sum(ums_drugs_pharm)) %>%
    ungroup()

}
#' General Pharmaceutical Services COVID-19 table 9 aggregations function
#'
#' This function takes the output from the GPhS 'national_extract' function
#' and aggregates it into community pharmacy services relating to the COVID-19
#' pandemic. Table is at national level data.
#'
#' @param national_extract The data resulting from using national_extract()
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_9 <- table_9(national_extract)

table_9 <- function(national_extract){

  table_9 <- national_extract %>%
    #limit financial year to years after the beginning of COVID-19 pandemic services
    #filter for community pharmacies only
    dplyr::filter(FINANCIAL_YEAR %!in% c("2015/2016","2016/2017","2017/2018","2018/2019","2019/2020"),
                  APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR, num_contractors,
                  NUM_CVDTESTREG_FEES, CVDTESTREG_FEES, NUM_CVDTESTSET_FEES,
                  CVDTESTSET_FEES, NUM_CVDTEST_KITS, CVDTEST_KITS, NUM_CVD_19_COSTS,
                  CVD_19_COSTS, NUM_CVD_19_VACCINE, CVD_19_VACCINE, NUM_CVD_PREM_REFRID,
                  CVD_PREM_REFRID, NUM_DISCHARGE_MEDS, DISCHARGE_MEDS, NUM_PPE_CLAIMS,
                  PPE_CLAIMS, num_home_del, num_home_del_total, home_del_cost_total) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    dplyr::summarise(`Community pharmacies` = sum(num_contractors),
                     `Pharmacies receiving COVID-19 Test Kit Registration fees` = sum(NUM_CVDTESTREG_FEES),
                     `Total cost of COVID-19 Test Kit Registration fees (GBP)` = sum(CVDTESTREG_FEES),
                     `Pharmacies receiving COVID-19 Test Kit Set up fees` = sum(NUM_CVDTESTSET_FEES),
                     `Total cost of COVID-19 Test Kit Set up fees (GBP)` = sum(CVDTESTSET_FEES),
                     `Pharmacies providing COVID-19 Testing Kits` = sum(NUM_CVDTEST_KITS),
                     `Total cost of COVID-19 Testing Kits fees (GBP)` = sum(CVDTEST_KITS),
                     `Pharmacies claiming COVID-19 related costs` = sum(NUM_CVD_19_COSTS),
                     `Total cost of COVID-19 related costs (GBP)` = sum(CVD_19_COSTS),
                     `Pharmacies providing COVID-19 vaccinations` = sum(NUM_CVD_19_VACCINE),
                     `Total cost of pharmacies providing COVID-19 vaccinations (GBP)` = sum(CVD_19_VACCINE),
                     `Pharmacies claiming COVID-19 premises and refrigeration costs` = sum(NUM_CVD_PREM_REFRID),
                     `Total cost of pharmacies claiming COVID-19 premises and refrigeration costs (GBP)` = sum(CVD_PREM_REFRID),
                     `Pharmacies claiming COVID-19 related PPE` = sum(NUM_PPE_CLAIMS),
                     `Total cost of pharmacies claiming COVID-19 related PPE (GBP)` = sum(PPE_CLAIMS),
                     `Pharmacies providing home deliveries` = sum(num_home_del),
                     `Total amount of home deliveries` = sum(num_home_del_total),
                     `Total cost of home deliveries (GBP)` = sum(home_del_cost_total)) %>%
    ungroup()
}
#' Function for table 10
#'
#' Function to create table from GPhS summary tables, using output from
#' 'national_extract' function, to get essential and enhanced services
#' for appliance contractors
#'
#'
#' @param national_extract the output of the national_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_10 <- table_10(national_extract)

table_10 <- function(national_extract){

  table_10 <- national_extract %>%
    #filter for appliance contractors only
    dplyr::filter(APPLIANCE_DISPENSER_HIST == "Y") %>%
    dplyr::select(FINANCIAL_YEAR, num_contractors, items_total,
                  costs_total, avg_monthly_items_pharm, num_eps_pharm, eps_items_total_pharm,
                  prof_fees_total_pharm, prof_fees_pharm, num_expensive_fees_pharm,
                  no_expensive_fees_pharm, expensive_fee_pharm,
                  num_oope_item_count_pharm, oope_item_count_pharm, oope_val_pharm,
                  num_add_fee_mf_pharm, add_fee_mf_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    dplyr::summarise(`Appliance contractors` = sum(num_contractors),
                     `Total prescription items dispensed` = sum(items_total),
                     `Total cost of prescription items dispensed (GBP)` = sum(costs_total),
                     `Average monthly items per appliance contractor` = avg_monthly_items_pharm,
                     `Appliance contractors dispensing via EPS` = sum(num_eps_pharm),
                     `Total items dispensed via the EPS` = sum(eps_items_total_pharm),
                     `Percentage of items dispensed via EPS (%)` =
                       ((`Total items dispensed via the EPS` / `Total prescription items dispensed`) * 100),
                     `Total number of dispensing fees received` = sum(prof_fees_total_pharm),
                     `Total value of dispensing fees received (GBP)` = sum(prof_fees_pharm),
                     `Appliance contractors receiving expensive item fees` = sum(num_expensive_fees_pharm),
                     `Total number of expensive item fees` = sum(no_expensive_fees_pharm),
                     `Total cost of expensive item fees (GBP)` = sum(expensive_fee_pharm),
                     `Appliance contractors receiving OOPE` = sum(num_oope_item_count_pharm),
                     `Total number of OOPE` = sum(oope_item_count_pharm),
                     `Total cost of OOPE (GBP)` = sum(oope_val_pharm),
                     `Appliance contractors receiving measure and fit fees` = sum(num_add_fee_mf_pharm),
                     `Total cost of measure and fit fees (GBP)` = sum(add_fee_mf_pharm)) %>%
    ungroup()
}
#' Function for table_11
#'
#' Function to create table from GPhS summary tables, using output from
#' 'national_extract' function, to get advanced services for appliance contractors
#'
#' @param national_extract the output of the national_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_11 <- table_11(national_extract)

table_11 <- function(national_extract){

  table_11 <- national_extract %>%
    #filter for appliance contractors only
    dplyr::filter(APPLIANCE_DISPENSER_HIST == "Y") %>%
    dplyr::select(FINANCIAL_YEAR, num_contractors, num_aur_home_pharm,
                  num_aur_home_total_pharm, num_aur_home_total_fee_pharm,
                  num_aur_prem_pharm, num_aur_prem_total_pharm,
                  num_aur_prem_total_fee_pharm, num_aur_all_pharm,
                  num_aur_all_total_pharm, num_aur_all_total_fee_pharm,
                  num_stoma_pharm, num_stoma_total_pharm, num_stoma_total_fee_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    #get totals for columns by Financial Year
    dplyr::summarise(`Appliance contractors` = sum(num_contractors),
                     `Appliance contractors providing home AURs` = sum(num_aur_home_pharm),
                     `Total amount of home AURs` = sum(num_aur_home_total_pharm),
                     `Total cost of home AURs (GBP)` = sum(num_aur_home_total_fee_pharm),
                     `Number of appliance contractors providing premises AURs` = sum(num_aur_prem_pharm),
                     `Total amount of premises AURs` = sum(num_aur_prem_total_pharm),
                     `Total cost of premises AURs (GBP)` = sum(num_aur_prem_total_fee_pharm),
                     `Number of appliance contractors providing any AURs` = sum(num_aur_all_pharm),
                     `Total amount of all AURs` = sum(num_aur_all_total_pharm),
                     `Total cost of all AURs (GBP)` = sum(num_aur_all_total_fee_pharm),
                     `Number of appliance contractors providing SAC` = sum(num_stoma_pharm),
                     `Total amount of SAC provided` = sum(num_stoma_total_pharm),
                     `Total cost of SAC (GBP)` = sum(num_stoma_total_fee_pharm)) %>%
    ungroup()

}
#' Function for table_12
#'
#' Function to create table from GPhS summary tables, using output from
#' 'national_extract' function, to get essential and enhanced services for
#' both community pharmacies and appliance contractors
#'
#' @param national_extract the output of the national_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_12 <- table_12(national_extract)

table_12 <- function(national_extract){

  table_12 <- national_extract %>%
    #include both community pharmacies and appliance contractors by not filtering
    #on appliance dispensing history
    dplyr::select(FINANCIAL_YEAR, num_contractors, items_total, costs_total,
                  num_eps_pharm, eps_items_total_pharm, prof_fees_total_pharm,
                  prof_fees_pharm, num_add_fee_mf_pharm, add_fee_mf_pharm,
                  num_expensive_fees_pharm, no_expensive_fees_pharm,
                  expensive_fee_pharm, num_oope_item_count_pharm,
                  oope_item_count_pharm, oope_val_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    dplyr::summarise(`Community pharmacies and appliance contractors` =
                       sum(num_contractors),
                     `Total prescription items dispensed` = sum(items_total),
                     `Total cost of prescription items dispensed (GBP)` = sum(costs_total),
                     `Contractors dispensing via EPS` = sum(num_eps_pharm),
                     `Total items dispensed via EPS` = sum(eps_items_total_pharm),
                     `Percentage of items dispensed via EPS (%)` =
                       ((`Total items dispensed via EPS` / `Total prescription items dispensed`) * 100),
                     `Total number of dispensing fees received` = sum(prof_fees_total_pharm),
                     `Total value of dispensing fees received (GBP)` = sum(prof_fees_pharm),
                     `Contractors receiving measure and fit fees` = sum(num_add_fee_mf_pharm),
                     `Total cost of measure and fit fees (GBP)` = sum(add_fee_mf_pharm),
                     `Contractors receiving expensive item fees` = sum(num_expensive_fees_pharm),
                     `Total number of expensive item fees` = sum(no_expensive_fees_pharm),
                     `Total cost of expensive item fees (GBP)` = sum(expensive_fee_pharm),
                     `Contractors receiving OOPE` = sum(num_oope_item_count_pharm),
                     `Total number of OOPE` = sum(oope_item_count_pharm),
                     `Total cost of OOPE (GBP)` = sum(oope_val_pharm),
    ) %>%
    ungroup()
}
#' Function for table_13
#'
#' Function to create table from GPhS summary tables, using output from
#' 'national_extract' function, to get advanced services for
#'  both community pharmacies and appliance contractors, including Appliance Use
#'  Review (AUR) and Stoma Appliance Customisation (SAC)
#'
#' @param national_extract the output of the national_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_13 <- table_13(national_extract)

table_13 <- function(national_extract){

  table_13 <- national_extract %>%
    #include both community pharmacies and appliance contractors by not filtering
    #on appliance dispensing history
    dplyr::select(FINANCIAL_YEAR, num_contractors, num_aur_home_pharm,
                  num_aur_home_total_pharm, num_aur_home_total_fee_pharm,
                  num_aur_prem_pharm, num_aur_prem_total_pharm,
                  num_aur_prem_total_fee_pharm, num_aur_all_pharm,
                  num_aur_all_total_pharm, num_aur_all_total_fee_pharm, num_stoma_pharm,
                  num_stoma_total_pharm, num_stoma_total_fee_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR) %>%
    #get totals for columns by Financial Year
    dplyr::summarise(`Community pharmacies and appliance contractors` = sum(num_contractors),
                     `Contractors providing home AURs` = sum(num_aur_home_pharm),
                     `Total amount of home AURs` = sum(num_aur_home_total_pharm),
                     `Total cost of home AURs (GBP)` = sum(num_aur_home_total_fee_pharm),
                     `Contractors providing premises AURs` = sum(num_aur_prem_pharm),
                     `Total amount of premises AURs` = sum(num_aur_prem_total_pharm),
                     `Total cost of premises AURs (GBP)` = sum(num_aur_prem_total_fee_pharm),
                     `Contractors providing any AURs` = sum(num_aur_all_pharm),
                     `Total amount of all AURs` = sum(num_aur_all_total_pharm),
                     `Total cost of all AURs (GBP)` = sum(num_aur_all_total_fee_pharm),
                     `Contractors providing SAC` = sum(num_stoma_pharm),
                     `Total amount of SAC provided` = sum(num_stoma_total_pharm),
                     `Total cost of SAC (GBP)` = sum(num_stoma_total_fee_pharm)) %>%
    ungroup()

}
#' Easy helper for table 14
#'
#' Function to create table from GPhS summary tables, using output from
#' 'icb_extract' function, to get community pharmacy activity at NHS region level
#'
#' @param icb_extract the output of the icb_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_14 <- table_14(icb_extract)

table_14 <- function(icb_extract){

  table_14 <- icb_extract %>%
    #filter to latest financial year (to be automated in future)
    #filter for community pharmacies only
    dplyr::filter(FINANCIAL_YEAR == max_fyr & APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR, REGION_CODE, REGION_NAME, num_contractors,
                  num_open_pharm, num_close_pharm, num_lps_pharm,
                  num_ind_pharm, num_multi_pharm, num_dist_selling_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR,
                    `Region Code` = REGION_CODE,
                    `Region Name` = REGION_NAME) %>%
    #get totals for columns by region
    dplyr::summarise(`Community pharmacies` = sum(num_contractors),
                     `Pharmacies opened` = sum(num_open_pharm),
                     `Pharmacies closed` = sum(num_close_pharm),
                     `LPS contractors` = sum(num_lps_pharm),
                     `Independent contractors` = sum(num_ind_pharm),
                     `Multiple contractors` = sum(num_multi_pharm),
                     `Distance selling pharmacies` = sum(num_dist_selling_pharm)) %>%
    ungroup()

}
#' Easy helper for table_15
#'
#' Function to create table from GPhS summary tables, using output from
#' 'icb_extract' function, to get community pharmacy activity at
#' Integrated Care Board (ICB) level
#'
#' @param icb_extract the output of the icb_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_15 <- table_15(icb_extract)

table_15 <- function(icb_extract){

  table_15 <- icb_extract %>%
    #filter to latest financial year (to be automated in future)
    #filter for community pharmacies only
    dplyr::filter(FINANCIAL_YEAR == max_fyr  & APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR, ICB_CODE, ICB_NAME, num_contractors,
                  num_open_pharm, num_close_pharm, num_lps_pharm,
                  num_ind_pharm, num_multi_pharm, num_dist_selling_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR,
                    `ICB Code` = ICB_CODE,
                    `ICB Name` = ICB_NAME) %>%
    #get totals for columns by Integrated Care Board (ICB)
    dplyr::summarise(`Community pharmacies` = sum(num_contractors),
                     `Pharmacies opened` = sum(num_open_pharm),
                     `Pharmacies closed` = sum(num_close_pharm),
                     `LPS contractors` = sum(num_lps_pharm),
                     `Independent contractors` = sum(num_ind_pharm),
                     `Multiple contractors` = sum(num_multi_pharm),
                     `Distance selling pharmacies` = sum(num_dist_selling_pharm)) %>%
    ungroup()

}
#' Easy helper for table_16
#'
#' Function to create table from GPhS summary tables, using output from
#' 'icb_extract' function, to get community pharmacy services at NHS region level
#'
#' @param icb_extract the output of the icb_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_16 <- table_16(icb_extract)

table_16 <- function(icb_extract){

  table_16 <- icb_extract %>%
    #filter to latest financial year (to be automated in future)
    #filter for community pharmacies only
    dplyr::filter(FINANCIAL_YEAR == max_fyr  & APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR, REGION_CODE, REGION_NAME, num_contractors,
                  items_total, num_eps_pharm, eps_items_total_pharm,
                  num_nms_pharm, num_nms_total, num_nms_fee_total,
                  num_flu_pharm, flu_items_total, flu_cost_total,
                  flu_fees_total, num_cpcs_drugs_pharm, cpcs_fees_pharm,
                  cpcs_drugs_pharm, num_hep_c_service_pharm, hep_c_service_pharm,
                  hep_c_kit_pharm, NUM_HYPTEN_INC, HYPTENSET_FEES, HYPTENCHECK_FEES,
                  HYPTEN_INC,NUM_SCS_CNSLT, SCS_CNSLT, SCS_NRTPROD_COST,
                  SCS_SETUP,avg_monthly_items_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR,
                    `Region Code` = REGION_CODE,
                    `Region Name` = REGION_NAME) %>%
    #get totals for columns by region
    dplyr::summarise(`Community pharmacies` = sum(num_contractors),
                     `Total number of items dispensed` = sum(items_total),
                     `Pharmacies dispensing via EPS` = sum(num_eps_pharm),
                     `Total number of EPS items dispensed` = sum(eps_items_total_pharm),
                     `Percentage of items dispensed via EPS (%)` =
                       ((`Total number of EPS items dispensed` / `Total number of items dispensed`) * 100),
                     `Pharmacies providing NMS` = sum(num_nms_pharm),
                     `Total number of NMS` = sum(num_nms_total),
                     `Total cost of NMS (GBP)` = sum(num_nms_fee_total),
                     `Pharmacies providing Flu vaccine service` = sum(num_flu_pharm),
                     `Total number of Flu vaccines administered` = sum(flu_items_total),
                     `Total cost of Flu vaccines administered (GBP)` = sum(flu_cost_total),
                     `Total fees paid for Flu vaccines (GBP)` = sum(flu_fees_total),
                     `Pharmacies providing CPCS` = sum(num_cpcs_drugs_pharm),
                     `Total fees paid for CPCS (GBP)` = sum(cpcs_fees_pharm),
                     `Total cost of drugs provided during CPCS (GBP)` = sum(cpcs_drugs_pharm),
                     `Pharmacies providing Hep C testing service` =
                       sum(num_hep_c_service_pharm),
                     `Total cost of provision of Hep C testing service (GBP)` =
                       sum(hep_c_service_pharm),
                     `Total cost of provision of Hep C Test Kit reimbursement (GBP)` =
                       sum(hep_c_kit_pharm),
                     `Pharmacies providing Hypertension service` =
                       sum(NUM_HYPTEN_INC),
                     `Total cost of set up of Hypertension service (GBP)` =
                       sum(HYPTENSET_FEES),
                     `Total cost of Hypertension service fees (GBP)` =
                       sum(HYPTENCHECK_FEES),
                     `Total cost of Hypertension service incentives (GBP)` =
                       sum(HYPTEN_INC),
                     `Pharmacies providing SCS` =
                       sum(NUM_SCS_CNSLT),
                     `Total cost of SCS set up fees (GBP)` =
                       sum(SCS_SETUP),
                     `Total cost of SCS consultations (GBP)` =
                       sum(SCS_CNSLT),
                     `Total cost of SCS products (GBP)` =
                       sum(SCS_NRTPROD_COST),
                     `Average yearly items per pharmacy` = sum(`Total number of items dispensed`)/sum(`Community pharmacies`)) %>%
    ungroup()

}
#' Easy helper for table_17
#'
#' Function to create table from GPhS summary tables, using output from
#' 'icb_extract' function, to get community pharmacy services at NHS region level
#'
#' @param icb_extract the output of the icb_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_17 <- table_17(icb_extract)

table_17 <- function(icb_extract){

  table_17 <- icb_extract %>%
    #filter to latest financial year (to be automated in future)
    #filter for community pharmacies only
    dplyr::filter(FINANCIAL_YEAR == max_fyr  & APPLIANCE_DISPENSER_HIST == "N") %>%
    dplyr::select(FINANCIAL_YEAR, ICB_CODE, ICB_NAME, num_contractors,
                  items_total, num_eps_pharm, eps_items_total_pharm,
                  num_nms_pharm, num_nms_total, num_nms_fee_total,
                  num_flu_pharm, flu_items_total, flu_cost_total,
                  flu_fees_total, num_cpcs_drugs_pharm, cpcs_fees_pharm,
                  cpcs_drugs_pharm, num_hep_c_service_pharm, hep_c_service_pharm,
                  hep_c_kit_pharm, NUM_HYPTEN_INC, HYPTENSET_FEES, HYPTENCHECK_FEES,
                  HYPTEN_INC,NUM_SCS_CNSLT, SCS_CNSLT, SCS_NRTPROD_COST,
                  SCS_SETUP, avg_monthly_items_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR,
                    `ICB Code` = ICB_CODE,
                    `ICB Name` = ICB_NAME) %>%
    #get totals for columns by ICB
    dplyr::summarise(`Community pharmacies` = sum(num_contractors),
                     `Total number of items dispensed` = sum(items_total),
                     `Pharmacies dispensing via EPS` = sum(num_eps_pharm),
                     `Total number of EPS items dispensed` = sum(eps_items_total_pharm),
                     `Percentage of items dispensed via EPS (%)` =
                       ((`Total number of EPS items dispensed` / `Total number of items dispensed`) * 100),
                     `Pharmacies providing NMS` = sum(num_nms_pharm),
                     `Total number of NMS` = sum(num_nms_total),
                     `Total cost of NMS (GBP)` = sum(num_nms_fee_total),
                     `Pharmacies providing Flu vaccine service` = sum(num_flu_pharm),
                     `Total number of Flu vaccines administered` = sum(flu_items_total),
                     `Total cost of Flu vaccines administered (GBP)` = sum(flu_cost_total),
                     `Total fees paid for Flu vaccines (GBP)` = sum(flu_fees_total),
                     `Pharmacies providing CPCS` = sum(num_cpcs_drugs_pharm),
                     `Total fees paid for CPCS (GBP)` = sum(cpcs_fees_pharm),
                     `Total cost of drugs provided during CPCS (GBP)` = sum(cpcs_drugs_pharm),
                     `Pharmacies providing Hep C testing service` =
                       sum(num_hep_c_service_pharm),
                     `Total cost of provision of Hep C testing service (GBP)` =
                       sum(hep_c_service_pharm),
                     `Total cost of provision of Hep C Test Kit reimbursement (GBP)` =
                       sum(hep_c_kit_pharm),
                     `Pharmacies providing Hypertension service` =
                       sum(NUM_HYPTEN_INC),
                     `Total cost of set up of Hypertension service (GBP)` =
                       sum(HYPTENSET_FEES),
                     `Total cost of Hypertension service fees (GBP)` =
                       sum(HYPTENCHECK_FEES),
                     `Total cost of Hypertension service incentives (GBP)` =
                       sum(HYPTEN_INC),
                     `Pharmacies providing SCS` =
                       sum(NUM_SCS_CNSLT),
                     `Total cost of SCS set up fees (GBP)` =
                       sum(SCS_SETUP),
                     `Total cost of SCS consultations (GBP)` =
                       sum(SCS_CNSLT),
                     `Total cost of SCS products (GBP)` =
                       sum(SCS_NRTPROD_COST),
                     `Average yearly items per pharmacy` = sum(`Total number of items dispensed`)/sum(`Community pharmacies`)) %>%
    ungroup()

}
#' Easy helper for table_18
#'
#' Function to create table from GPhS summary tables, using output from
#' 'icb_extract' function, to get services at NHS region level for both
#' community pharmacies and appliance contractors
#'
#' @param icb_extract the output of the icb_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_18 <- table_18(icb_extract)

table_18 <- function(icb_extract){

  table_18 <- icb_extract %>%
    #filter to latest financial year (to be automated in future)
    dplyr::filter(FINANCIAL_YEAR == max_fyr ) %>%
    dplyr::select(FINANCIAL_YEAR, REGION_CODE, REGION_NAME, num_contractors,
                  num_aur_home_pharm, num_aur_prem_pharm, num_aur_all_pharm,
                  num_aur_home_total_pharm, num_aur_prem_total_pharm,
                  num_aur_all_total_pharm, num_aur_home_total_fee_pharm,
                  num_aur_prem_total_fee_pharm, num_aur_all_total_fee_pharm,
                  num_stoma_pharm, num_stoma_total_pharm, num_stoma_total_fee_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR,
                    `Region Code` = REGION_CODE,
                    `Region Name` = REGION_NAME) %>%
    #get totals for columns by region
    dplyr::summarise(`Community pharmacies and appliance contractors` = sum(num_contractors),
                     `Contractors providing home AURs` = sum(num_aur_home_pharm),
                     `Contractors providing premises AURs` = sum(num_aur_prem_pharm),
                     `Contractors providing any AURs` = sum(num_aur_all_pharm),
                     `Total number of home AURs` = sum(num_aur_home_total_pharm),
                     `Total number of premises AURs` = sum(num_aur_prem_total_pharm),
                     `Total number of AURs` = sum(num_aur_all_total_pharm),
                     `Total cost of home AURs (GBP)` = sum(num_aur_home_total_fee_pharm),
                     `Total cost of premises AURs (GBP)` = sum(num_aur_prem_total_fee_pharm),
                     `Total cost of AURs (GBP)` = sum(num_aur_all_total_fee_pharm),
                     `Contractors providing SAC` = sum(num_stoma_pharm),
                     `Total amount of SAC` = sum(num_stoma_total_pharm),
                     `Total cost of SAC (GBP)` = sum(num_stoma_total_fee_pharm)) %>%
    ungroup()

}
#' Easy helper for table_19
#'
#' Function to create table from GPhS summary tables, using output from
#' 'icb_extract' function, to get services at Integrated Care Board (ICB) level
#'  for community pharmacies and appliance contractors
#'
#' @param icb_extract the output of the icb_extract function
#'
#' @import dplyr
#'
#' @export
#'
#' @examples
#' table_19 <- table_19(icb_extract)

table_19 <- function(icb_extract){

  table_19 <- icb_extract %>%
    #filter to latest financial year (to be automated in future)
    dplyr::filter(FINANCIAL_YEAR == max_fyr ) %>%
    dplyr::select(FINANCIAL_YEAR, ICB_CODE, ICB_NAME, num_contractors,
                  num_aur_home_pharm, num_aur_prem_pharm, num_aur_all_pharm,
                  num_aur_home_total_pharm, num_aur_prem_total_pharm,
                  num_aur_all_total_pharm, num_aur_home_total_fee_pharm,
                  num_aur_prem_total_fee_pharm, num_aur_all_total_fee_pharm,
                  num_stoma_pharm, num_stoma_total_pharm, num_stoma_total_fee_pharm) %>%
    dplyr::group_by(`Financial Year` = FINANCIAL_YEAR,
                    `ICB Code` = ICB_CODE,
                    `ICB Name` = ICB_NAME) %>%
    #get totals for columns by ICB
    dplyr::summarise(`Community pharmacies and appliance contractors` = sum(num_contractors),
                     `Contractors providing home AURs` = sum(num_aur_home_pharm),
                     `Contractors providing premises AURs` = sum(num_aur_prem_pharm),
                     `Contractors providing any AURs` = sum(num_aur_all_pharm),
                     `Total number of home AURs` = sum(num_aur_home_total_pharm),
                     `Total number of premises AURs` = sum(num_aur_prem_total_pharm),
                     `Total number of AURs` = sum(num_aur_all_total_pharm),
                     `Total cost of home AURs (GBP)` = sum(num_aur_home_total_fee_pharm),
                     `Total cost of premises AURs (GBP)` = sum(num_aur_prem_total_fee_pharm),
                     `Total cost of AURs (GBP)` = sum(num_aur_all_total_fee_pharm),
                     `Contractors providing SAC` = sum(num_stoma_pharm),
                     `Total number of SAC` = sum(num_stoma_total_pharm),
                     `Total cost of SAC (GBP)` = sum(num_stoma_total_fee_pharm)) %>%
    ungroup()

}
#' Function for table 20
#'
#' Function to create table from GPhS summary tables, using NHS Resolution
#' data for current year, appended to previous year's Resolutions data on
#' application outcomes.
#'
#' @param national_extract the output of the national_extract function
#'
#' @import dplyr
#' @import readxl
#' @import tibble
#'
#'
#' @export
#'
#' @examples
#' table_20 <- table_20()

table_20 <- function(){
  #import previous years' data from previous years GPhS summary tables table 9
  #code assumes data is kept in R folder in GPhS project directory
  #change file path if stored elsewhere
  prev_resolution_data <- readxl::read_xlsx("Ref/Resolutions/gps_2223_summary_tables_v001.xlsx",
                                            sheet = 22,
                                            range = "A5:D155",
                                            col_names = TRUE)
  #change missing values from NA back to 0
  prev_resolution_data <- prev_resolution_data %>%
    dplyr::mutate(across(where(is.numeric), ~ ifelse(is.na(.), 0, .)))
  #import new data for 2022/23
  cur_resolution_data <- readxl::read_xlsx("Ref/Resolutions/2023-24 Pharmacy data.xlsx",
                                           col_names = TRUE)
  #change missing values from NA back to 0
  cur_resolution_data <- cur_resolution_data %>%
    dplyr::mutate(across(where(is.numeric), ~ ifelse(is.na(.), 0, .))) %>%
    mutate(
      `Application decision` = ifelse(
        `Application decision` == "refused application",
        "Application refused",
        "Application granted"
      )
    )
  #apply previous year's formatting to new data
  #create table of controlled decisions and table of noncontrolled decisions
  #then append to each other using rbind() function
  controlled <- cur_resolution_data %>%
    dplyr::filter(Locality == "Controlled") %>%
    dplyr::mutate(`Application Decision` = `Application decision`) %>%
    dplyr::select(`Financial Year`, Locality, `Application Decision`, Number) %>%
    dplyr::group_by(`Financial Year`, Locality, `Application Decision`) %>%
    dplyr::summarise(`Total Decisions` = sum(Number)) %>%
    dplyr::ungroup()
  noncontrolled <- cur_resolution_data %>%
    dplyr::filter(Locality == "Non-Controlled") %>%
    dplyr::mutate(`Application Decision` = `Application decision`) %>%
    dplyr::select(`Financial Year`, Locality, `Application Decision`, Number) %>%
    dplyr::group_by(`Financial Year`, Locality, `Application Decision`) %>%
    dplyr::summarise(`Total Decisions` = sum(Number)) %>%
    dplyr::ungroup()
  #append controlled data to noncontrolled data
  locality_data <- rbind(controlled, noncontrolled)
  #create totals of both controlled and noncontrolled decisions
  decision_totals <- locality_data %>%
    dplyr::select(`Financial Year`, `Application Decision`, `Total Decisions`) %>%
    tibble::add_column(Locality = "Total") %>%
    dplyr::group_by(`Financial Year`, Locality,`Application Decision`) %>%
    dplyr::summarise(`Total Decisions` = sum(`Total Decisions`)) %>%
    ungroup()
  #append totals data to Locality data
  cur_resolution_data_final <- rbind(locality_data, decision_totals)
  #append 2021/22 data to previous years' data
  table_20 <- rbind(prev_resolution_data, cur_resolution_data_final) %>%
    complete(
      `Application Decision`,
      nesting(`Financial Year`, Locality),
      fill = list(`Total Decisions` = 0)
    ) %>%
    select(
      `Financial Year`, `Locality`, `Application Decision`, `Total Decisions`
    ) %>%
    #group_by(`Financial Year`) %>%
    arrange(
      `Financial Year`,
      match(
        Locality,
        c(
          "Controlled",
          "Non-Controlled",
          "Total"
        )
      ),
      match(
        `Application Decision`,
        c(
          "Application granted",
          "Application refused",
          "Appeal withdrawn - application granted",
          "Appeal withdrawn - application refused",
          "Remit back to NHSE"
        )
      )
    )
  return(table_20)
}
#Vis
#group chart without rounding for pharmacy chart
group_chart_hc_unround <- function(data,
                                   x,
                                   y,
                                   type = "line",
                                   group,
                                   xLab = NULL,
                                   yLab = NULL,
                                   title = NULL,
                                   dlOn = TRUE,
                                   currency = FALSE,
                                   marker = TRUE) {
  # this function creates a group bar chart with NHSBSA data vis standards
  # applied. includes datalabel formatter to include "£" if needed.

  x <- rlang::enexpr(x)
  y <- rlang::enexpr(y)

  group <- rlang::enexpr(group)

  # set font to arial
  font <- "Arial"

  # get number of groups. max number of groups is 9 for unique colors
  num_groups <- length(unique(data[[group]]))

  # define a set of colors
  colors <- c("#005eb8", "#ed8b00", "#009639", "#8a1538", "#00a499")

  # if there are more groups than colors, recycle the colors
  if (num_groups > length(colors)) {
    colors <- rep(colors, length.out = num_groups)
  }


  #if there is a 'Total' groups ensure this takes the color black
  if ("Total" %in% unique(data[[group]])) {
    #identify index of "total" group
    total_index <- which(sort(unique(data[[group]])) == "Total")

    # add black to location of total_index
    colors <-
      c(colors[1:total_index - 1], "#000000", colors[total_index:length(colors)])
  }

  # subset the colors to the number of groups
  #colors <- ifelse(unique(data[[group]]) == "Total", "black", colors[1:num_groups])

  # check currency argument to set symbol
  # dlFormatter <- highcharter::JS(
  #   paste0(
  #     "function() {
  #   var ynum = this.point.y;
  #   var options = { maximumSignificantDigits: 3, minimumSignificantDigits: 3 };
  #     if (",
  #   tolower(as.character(currency)),
  #   ") {
  #     options.style = 'currency';
  #     options.currency = 'GBP';
  #     }
  #     if (ynum >= 1000000000) {
  #       options.maximumSignificantDigits = 4;
  #       options.minimumSignificantDigits = 4;
  #     }else {
  #      options.maximumSignificantDigits = 3;
  #       options.minimumSignificantDigits = 3;
  #     }
  #   return ynum.toLocaleString('en-GB', options);
  # }"
  #   )
  # )


  # ifelse(is.na(str_extract(!!y, "(?<=\\().*(?=,)")),!!y,str_extract(!!y, "(?<=\\().*(?=,)")),

  # check chart type to set grid lines
  gridlineColor <- if (type == "line")
    "#e6e6e6"
  else
    "transparent"

  # check chart type to turn on y axis labels
  yLabels <- if (type == "line")
    TRUE
  else
    FALSE

  # highchart creation
  chart <- highcharter::highchart() |>
    highcharter::hc_chart(style = list(fontFamily = font)) |>
    highcharter::hc_colors(colors) |>
    # add only series
    highcharter::hc_add_series(
      data = data,
      type = type,
      marker = list(enabled = marker),
      highcharter::hcaes(
        x = !!x,
        y = !!y,
        group = !!group
      ),
      groupPadding = 0.1,
      pointPadding = 0.05,
      dataLabels = list(
        enabled = dlOn,
        # formatter = dlFormatter,
        style = list(textOutline = "none")
      )
    ) |>
    highcharter::hc_xAxis(type = "category",
                          title = list(text = xLab)) |>
    # turn off y axis and grid lines
    highcharter::hc_yAxis(
      title = list(text = yLab),
      labels = list(enabled = yLabels),
      gridLineColor = gridlineColor,
      min = 0
    ) |>
    highcharter::hc_title(text = title,
                          style = list(fontSize = "16px",
                                       fontWeight = "bold")) |>
    highcharter::hc_legend(enabled = TRUE) |>
    highcharter::hc_tooltip(enabled = FALSE) |>
    highcharter::hc_credits(enabled = TRUE)

  # explicit return
  return(chart)
}

#infoBox_border() --------------------------------------------------------------
#function to create info box in NHS colour scheme with border
#example: infobox_border(" ", text = "Text goes here", width = 100%)

infoBox_border <- function(
    header = "Header here",
    text = "More text here",
    backgroundColour = "#ccdff1",
    borderColour = "#005EB8",
    width = "31%",
    fontColour = "black") {

  #set handling for when header is blank
  display <- "block"

  if(header == "") {
    display <- "none"
  }

  paste(
    "<div class='infobox_border' style = 'border: 1px solid ", borderColour,"!important;
  border-left: 5px solid ", borderColour,"!important;
  background-color: ", backgroundColour,"!important;
  padding: 10px;
  width: ", width,"!important;
  display: inline-block;
  vertical-align: top;
  flex: 1;
  height: 100%;'>
  <p style = 'color: ", fontColour, ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;
  display: ", display,";'>",
  header, "</p>
  <p style = 'color: ", fontColour, ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>", text, "</p>
</div>"
  )
}

#infoBox_no_border -------------------------------------------------------------
#function to create info box in NHS colour scheme without border
#example: infoBox_no_border("", text = "<b>Text goes here.</b>", width = "100%")

infoBox_no_border <- function(
    header = "Header here",
    text = "More text here",
    backgroundColour = "#005EB8",
    width = "31%",
    fontColour = "white") {

  #set handling for when header is blank
  display <- "block"

  if(header == "") {
    display <- "none"
  }

  paste(
    "<div class='infobox_no_border',
    style = 'background-color: ",backgroundColour,
    "!important;padding: 10px;
    width: ",width,";
    display: inline-block;
    vertical-align: top;
    flex: 1;
    height: 100%;'>
  <p style = 'color: ", fontColour, ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;
  display: ", display,";'>",
  header, "</p>
  <p style = 'color: ", fontColour, ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>", text, "</p>
</div>"
  )
}

#get_download_button() ---------------------------------------------------------
#function to create button to display in markdown file,
#assigning data used in chart to this button will download data as csv
#when button is clicked in html output file webpage
#example: get_download_button(title = "Download chart data",
#                             data = figure_1_data,
#                             filename = "figure_1")

get_download_button <-
  function(data = data,
           title = "Download chart data",
           filename = "data") {
    dt <- datatable(
      data,
      rownames = FALSE,
      extensions = 'Buttons',
      options = list(
        searching = FALSE,
        paging = TRUE,
        bInfo = FALSE,
        pageLength = 1,
        dom = '<"datatable-wrapper"B>',
        buttons = list(
          list(
            extend = 'csv',
            text = title,
            filename = filename,
            className = "nhs-button-style"
          )
        ),
        initComplete = JS(
          "function(settings, json) {",
          "$(this.api().table().node()).css('visibility', 'collapse');",
          "}"
        )
      )
    )

    return(dt)
  }
