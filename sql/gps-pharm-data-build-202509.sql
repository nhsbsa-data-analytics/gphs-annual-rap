/*	<TITLE>	gps-pharm-data-build.sql

	<DESCRIPTION>	builds staging tables for the General Pharmaceutical Services National Statistic publication

	<DETAILS>	this code builds a series of staging tables to aid performance and reduce computational burden
				on DWCP. build staging tables:
				tdim	-	pulls out year_month and financial_year for all relevant data. set year_months based on latest month
							available. possible to set replacement var for R to find.
				dorg	-	pull out all organisational attributes required for the publication. this includes setting contractor
							type flags as at latest month they appear in dim.hs_level_5_flat_dim, along with parent orgs.
				pd1		-	gets fact data already aggregated to account level. some fields missing from this table such as flu,
							stoma customisation, and number of AURs.
				fact	-	go to lowest level fact to get stoma fees, as well as some other totals to cross check between tables.
				flu		-	go to aml.vf_pb_data_fact to get data for flu submissions. this data now comes from MYS, hosted in DW.
				dpc		-	go to dpc tables in scd2 to get number of AURs at account level.

	<CREATED>	03/11/2020

	<CREATED BY>	MAWIL

	<AMENDED> 12/10/2021 KIGRA for 2020/21 release

				added home delivery to dpc
				removed local office from dorg and amended at to stp
			-- 15/10/2021 KIGRA
				added Urgent Medicine Supply drugs, Urgent Medicine Supply fees, Urgent Medicine charges, Community Pharmacist Consultation Service drugs,
				Community Pharmacist Consultation Service fees, Community Pharmacist Consultation Service charges, Community Pharmacist Consultation Service Sign-Up fees,
				Hep C Provision of Testing Service, Hep C Test Kit Reimbursement, Methadone Fees, CD Schedule 2 Fees, CD Schedule 3 Fees, 2A - extemporaneously dispensed preparations,
				measure and fit, Expensive Item fees, No of Expensive Item fees, No of Out of pocket expenses, Out of pocket expenses and code for Serious Shortage Protocol Fees
	<AMENDED> 06/10/2022 KIGRA for 2021/22 release
				amended stp to icb
				added Discharge Medicines Services, Hypertension Case-Finding service, COVID-19 Test Kit Registration Fees,COVID-19 Test Kit Set up Fees, COVID-19 Testing Kits,
				COVID-19 related costs, COVID-19 vaccinations, COVID-19 premises and refrigeration costs,
				COVID-19 related PPE, home deliveries, code for smoking cessation was added though no data for this period
	<AMENDED> 19/09/2023 KIGRA for 2022/23 release
				added tier 1 contraception set up (both expense heads), tier 1 contraception product cost, tier 1 contraception consultations
	<AMENDED> 18/09/2024 KIGRA for 2023/24 release
				added Pharmacy first intial fees, consultation fees, payment, months, vat, umsmi deductions, umsmi remuneration and umsmi reimbursent
	<AMENDED> 10/08/2025 KIGRA for 2024/25 release
				correct PD1 to remove consultation only items
	<AMENDED> 29/9/2025 KIGRA for 2024/25 release to add items and nic for pharmacy first schemes including contraception and hyp for 25/26


*/

/*	create staging tables in own schema to aid performance, build time dim first	*/
/*	</TDIM_DROP/>	*/
drop table 		gps_tdim_202509	cascade constraints	purge;
/*	</TDIM_CREATE/>	*/
create table	gps_tdim_202509	compress for		query high	as

with

tdim	as	(
	select
		year_month
		,financial_year
		,count(distinct	year_month)	over(partition by	financial_year)	as	month_count
	from
		dim.year_month_dim
	where	1	=	1
		and	year_month	between	201504	and	MGMT.PKG_PUBLIC_DWH_FUNCTIONS.f_get_latest_period('EPACT2')
)

/*	only give year_months where we have a full financial year	*/
select	*	from	tdim	where	month_count	=	12
;

/*	build org dim, need latest indicator in the year. may need to add getting latest STP/Region	*/
/*	</DORG_DROP/>	*/
drop table 		gps_dorg_202509	cascade constraints	purge;
/*	</DORG_CREATE/>	*/
create table	gps_dorg_202509	compress for		query high	as

with

dorg	as	(
	select
		tdim.financial_year
		,l5.year_month
		,l5.lvl_5_oupdt
		,l5.lvl_5_ou
		,l5.lvl_5_ltst_alt_cde			as	dispenser_code
		/*	contractor type flags	*/
		,first_value(l5.pharm_app_hist_ind_desc)		over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	pharm_app_hist_ind_desc
		,first_value(l5.appliance_dispenser_hist)		over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	appliance_dispenser_hist
		,first_value(l5.lps_dispenser_hist)				over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	lps_dispenser_hist
		,first_value(l5.dist_selling_dispenser_hist)	over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	dist_selling_dispenser_hist
		,first_value(l5.disp_selling_hist_ind_desc)		over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	disp_selling_hist_ind_desc
		,first_value(l5.cont_type_hist_ind)				over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	cont_type_hist_ind
		,first_value(l5.cont_type_hist_ind_desc)		over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	cont_type_hist_ind_desc
		/*	parent	org	information, 27 area teams, 14 local teams, 4 regions until 20200331. will need amending for 202021 publication	*/
		,first_value(l5.hs_area_team_ltst_nm)			over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	icb_name			/*	fix for 202021 release	*/
		,first_value(l5.hs_area_team_ltst_alt_cde)		over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	icb_code			/*	fix for 202021 release	*/
		/*,first_value(l5.hs_local_office_plus_cde)		over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	local_office_plus_code	no longer needed for 202021 release	*/
		,first_value(l5.hs_region_ltst_nm)				over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	region_name				/*	fix for 202021 release	*/
		,first_value(l5.hs_region_ltst_alt_cde)			over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	region_code				/*	fix for 202021 release	*/
		/*	start and close date to determine openings and closures	*/
		,l5.lvl_5_ltst_date_inact		as	close_date_ltst
		,first_value(l5.lvl_5_hist_date_inact)			over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	close_date_hist
		,first_value(l5.lvl_5_hist_date_started)		over(partition by	tdim.financial_year,l5.lvl_5_ltst_alt_cde	order by	l5.year_month	desc)	as	start_date_hist
	from
		dim.hs_dy_level_5_flat_dim	l5
	inner join
		gps_tdim_202509	tdim
		on	l5.year_month	=	tdim.year_month
	where	1	=	1
		and l5.pharm_app_hist_ind_desc	!=	'UNKNOWN'	/*	give all L5 records with pharmacy or appliance */
		and	l5.hs_ctry_ou				=	1			/*	only want english pharmacy and appliance	*/
		and	l5.data_added_by_dental		=	'N'			/*	remove dental contract information	*/
		and	l5.private_dispenser_hist	=	'N'			/*	remove private dispensers	*/
		and	l5.ooh_dispenser_hist		=	'N'			/*	remove	out of hours contractors	*/
)

select
	financial_year
	,year_month
	,lvl_5_oupdt
	,lvl_5_ou
	,dispenser_code
	,pharm_app_hist_ind_desc
	,appliance_dispenser_hist
	,lps_dispenser_hist
	,dist_selling_dispenser_hist
	,disp_selling_hist_ind_desc
	,cont_type_hist_ind
	,cont_type_hist_ind_desc
	/*	case statement to label contractors as independents or multiples	*/
	,case	when	cont_type_hist_ind	in	('01','05')	then	'Independent'
			when	cont_type_hist_ind	=	'07'		then	'Appliance contractor'
			else	'Multiple contractor'
	end	as	contractor_type
	,icb_name
	,icb_code
	--,local_office_plus_code - not longer needed 20/21
	--,substr(local_office_plus_code,1,length(local_office_plus_code)-6)	as	local_office_name
	--,substr(local_office_plus_code,-4,3)								as	local_office_code
	,region_name
	,region_code
	/*	need to convert to date to character to avoid issue caused with datetimes in R	*/
	,to_char(close_date_ltst,'DD/MM/YYYY')								as	close_date_ltst
	,to_char(close_date_hist,'DD/MM/YYYY')								as	close_date_hist
	,to_char(start_date_hist,'DD/MM/YYYY')								as	start_date_hist
from
	dorg
;

/*	build PD1 dim. this is basically just going to PD1 table and pulling data out	*/
/*	</PD1_DROP/>	*/
drop table 		gps_pd1_202509	cascade constraints	purge;
/*	</PD1_CREATE/>	*/
create table	gps_pd1_202509	compress for		query high	as

with

/*	PD1 NOT already at account level due to LPD and late submissions
	need to aggregate still.
*/
pd1	as	(
	select
		tdim.financial_year
		,pd1.year_month
		,disp_type	as	disp_oupdt_type /* renamed to avoid confusion */
		,disp_id
		,dispenser_code
		,sum(item_count)	as	pd1_items
		,sum(pay_nic)/100	as	pd1_nic
		,sum(prof_fees)/100	as	pd1_prof_fees
		,sum(num_prof_fees)	as	pd1_num_prof_fees
		,sum(mur_fee)/100	as	pd1_mur_fees
		,sum(mur_num)		as	pd1_num_mur
		,sum(nms_fee)/100	as	pd1_nms_fees
		,sum(nms_num)		as	pd1_num_nms
		,sum(aur_home)/100	as	pd1_aur_home
		,sum(aur_prem)/100	as	pd1_aur_prem
		,sum(cd_fee)/100 as pd1_cd_fee
		,sum(cd_sched2_fee)/100 as pd1_cd_sched2_fee
		,sum(cd_sched3_fee)/100 as pd1_cd_sched3_fee
		,sum(add_fee_2a)/100 as pd1_add_fee_2a
		,sum(add_fee_mf)/100 as pd1_add_fee_mf
		,sum(expensive_fee)/100 as pd1_expensive_fee
		,sum(expensive_px_fees) as pd1_expensive_px_fees
		,sum(oope_item_count) as pd1_oope_item_count
		,sum(oope_val)/100 as pd1_oope_val
		,sum(ssp_fees) as pd1_ssp_fees

	from
		aml.bsa_disp_pd1_fact	pd1
	inner join
		gps_tdim_202509	tdim
		on	pd1.year_month	=	tdim.year_month
	where	1	=	1
		and	pd1.country_code	=	1
		and pd1.private_ind		=	0
		and	pd1.oohc_ind		=	0
		and	pd1.source_system	!=	'PCD'	/*	excludes items processed by the new controlled drugs system	*/
		and pd1.mys_service_type = 'N'
		or pd1.mys_service_type is null
	group by
		tdim.financial_year
		,pd1.year_month
		,disp_type
		,disp_id
		,dispenser_code
)

select
	financial_year
	,year_month
	,disp_oupdt_type
	,disp_id
	,dispenser_code
	,pd1_items
	,pd1_nic
	,pd1_prof_fees
	,pd1_num_prof_fees
	,pd1_mur_fees
	,pd1_num_mur
	,pd1_nms_fees
	,pd1_num_nms
	,pd1_aur_home
	,pd1_aur_prem
	,pd1_cd_fee
	,pd1_cd_sched2_fee
	,pd1_cd_sched3_fee
	,pd1_add_fee_2a
	,pd1_add_fee_mf
	,pd1_expensive_fee
	,pd1_expensive_px_fees
	,pd1_oope_item_count
	,pd1_oope_val
	,pd1_ssp_fees

	/*	get distinct count of months for number of months active	*/
	,count(distinct	year_month)	over(partition by	financial_year,dispenser_code)	as	month_count
from
	pd1
where	1	=	1
;

/*	go to LLF to pull out stoma fees and EPS items. can also pull out other fields as a cross check	*/
/*	</FACT_DROP/>	*/
drop table 		gps_fact_202509	cascade constraints	purge;
/*	</FACT_CREATE/>	*/
create table	gps_fact_202509	compress for		query high	as

with

/*	LLF is at element level, need to aggregate to account level before joining	*/
fact	as	(
	select
		tdim.financial_year
		,fact.year_month
		,fact.disp_oupdt_type
		,fact.disp_id
		,fact.dispenser_code
		,sum(fact.custom_stoma_fees)/100		as	custom_stoma_fees
		,sum(fact.num_custom_stoma_fees)		as	num_custom_stoma_fees
		,sum(fact.item_custom_stoma_fees)/100	as	item_custom_stoma_fees
		,sum(fact.item_num_custom_stoma_fees)	as	item_num_custom_stoma_fees
		,sum(case	when	fact.eps_flag	=	'Y'	then	fact.item_count	else	0	end)	as	eps_items
		,sum(fact.item_count)					as	fact_items
		,sum(fact.item_pay_dr_nic)/100			as	fact_dr_nic
		,sum(fact.item_pay_nic)/100				as	fact_nic
		,sum(fact.item_num_prof_fees)			as	fact_num_prof_fees
		,sum(fact.item_prof_fees)/100			as	fact_prof_fees

	from
		aml.px_form_item_elem_comb_fact_av	fact
	inner join
		gps_tdim_202509	tdim
		on	fact.year_month	=	tdim.year_month
	where	1	=	1
		and	fact.dispenser_country_ou	=	1
		and	fact.account_type			in	(5,8)
		--  regular exlusions
        and fact.PAY_DA_END         =   'N' -- excludes disallowed items
        and fact.PAY_ND_END         =   'N' -- excludes not dispensed items
        and fact.PAY_RB_END         =   'N' -- excludes referred back items
        and fact.CD_REQ             =   'N' -- excludes controlled drug requisitions
        and fact.OOHC_IND           =   0   -- excludes out of hours dispensing
        and fact.PRIVATE_IND        =   0   -- excludes private dispensers
        and fact.IGNORE_FLAG        =   'N' -- excludes LDP dummy forms
        and fact.PRESC_TYPE_PRNT    not in  (8,54)  -- excludes private and pharmacy prescribers
		and nvl(fact.consult_only_ind, 'N') = 'N' -- excludes pharmacy first consultations
	group by
		tdim.financial_year
		,fact.year_month
		,fact.disp_oupdt_type
		,fact.disp_id
		,fact.dispenser_code
)

select	*	from	fact
;

/*	create flu dim. not every contractor will have a flu claim, and only exist for flu months	*/
/*	</FLU_DROP/>	*/
drop table 		gps_flu_202509	cascade constraints	purge;
/*	</FLU_CREATE/>	*/
create table	gps_flu_202509	compress for		query high	as

with

flu	as	(
	select
		flu.year_month
		,flu.level_5_oupdt
		,flu.level_5_ou
		,sum(flu.quantity)		as	flu_items
		,sum(flu.drug_cost)/100	as	flu_cost
		,sum(flu.fees_cost)/100	as	flu_fees
	from
		aml.vf_pb_data_fact flu
	inner join
		gps_tdim_202509	tdim
		on	flu.year_month	=	tdim.year_month
	where	1	=	1
	group by
		flu.year_month
		,flu.level_5_oupdt
		,flu.level_5_ou
)

select	*	from	flu
;

/*	query scd2 dpc tables to pull out number of aur home and prem, along with some check columns	*/
/*	</DPC_DROP/>	*/
drop table 		gps_dpc_202509	cascade constraints	purge;
/*	</DPC_CREATE/>	*/
create table	gps_dpc_202509	compress for		query high	as

with

dpc	as	(
	select
		pr.schedule_date				as	year_month
		,pay.ocs_code					as	dispenser_code
		,sum(nvl(cc.aur_home_num,0))	as	dpc_num_aur_home
		,sum(nvl(ilp322.amount,0))/100	as	dpc_aur_home
		,sum(nvl(cc.aur_prem_num,0))	as	dpc_num_aur_prem
		,sum(nvl(ilp320.amount,0))/100	as	dpc_aur_prem
		,sum(nvl(ilp145.amount,0))		as	dpc_home_del
		,sum(nvl(ilp145.amount,0))/6	as	dpc_num_home_del
		,sum(nvl(ilp164.amount,0)) 		as dpc_ums_drugs
		,sum(nvl(ilp166.amount,0))		as dpc_ums_fees
		,sum(nvl(ilp168.amount,0))		as dpc_ums_charges
		,sum(nvl(ilp178.amount,0)) 		as dpc_cpcs_drugs
		,sum(nvl(ilp180.amount,0)) 		as dpc_cpcs_fees
		,sum(nvl(ilp182.amount,0))		as dpc_cpcs_charges
		,sum(nvl(ilp184.amount,0))		as dpc_cpcs_signup
		,sum(nvl(ilp193.amount,0))		as dpc_hep_c_service
		,sum(nvl(ilp195.amount,0)) 		as dpc_hep_c_kit
		,sum(nvl(ilp197.amount,0)) 		as dpc_ppe_claims
		,sum(nvl(ilp201.amount,0)) 		as dpc_discharge_meds
		,sum(nvl(ilp208.amount,0)) 		as dpc_cvd_19_costs
		,sum(nvl(ilp211.amount,0)) 		as dpc_cvdtestreg_fees
		,sum(nvl(ilp212.amount,0)) 		as dpc_cvdtestset_fees
		,sum(nvl(ilp213.amount,0)) 		as dpc_gprefpath_fees
		,sum(nvl(ilp215.amount,0)) 		as dpc_cvd_19_vaccine
		,sum(nvl(ilp217.amount,0)) 		as dpc_cvd_prem_refrid
		,sum(nvl(ilp219.amount,0)) 		as dpc_cvdtest_kits
		,sum(nvl(ilp261.amount,0)) 		as dpc_hyptenset_fees
		,sum(nvl(ilp263.amount,0)) 		as dpc_hyptencheck_fees
		,sum(nvl(ilp265.amount,0)) 		as dpc_hypten_inc
		,sum(nvl(ilp267.amount,0)) 		as dpc_scs_setup
		,sum(nvl(ilp269.amount,0)) 		as dpc_scs_cnslt
		,sum(nvl(ilp271.amount,0)) 		as dpc_scs_nrtprod_cost
		,sum(nvl(ilp273.amount,0)) 		as dpc_scs_nrtprod_charge
		,sum(nvl(ilp288.amount,0)) 		as dpc_t1c_set_up
		,sum(nvl(ilp284.amount,0)) 		as dpc_t1c_prod_cost
		,sum(nvl(ilp282.amount,0)) 		as dpc_t1c_consult
		,sum(nvl(ilp303.amount,0)) 		as dpc_pfcp_optin
		,sum(nvl(ilp305.amount,0)) 		as dpc_pfcp_fees
		,sum(nvl(ilp306.amount,0)) 		as dpc_pfcp_payment
		,sum(nvl(ilp307.amount,0)) 		as dpc_pfcp_months
		,sum(nvl(ilp308.amount,0)) 		as dpc_pfcp_vat
		,sum(nvl(ilp317.amount,0)) 		as dpc_pfcp_umsmideduct
		,sum(nvl(ilp315.amount,0)) 		as dpc_pfcp_umsmiremuneration
		,sum(nvl(ilp313.amount,0)) 		as dpc_pfcp_umsmireimbursement
		,sum(nvl(cc.disp_items_num,0))	as	dpc_items
		,sum(nvl(cc.tot_num_pxs_std,0) + nvl(cc.tot_num_pxs_zd,0))	as	dpc_num_prof_fees
	from
		scd2.scd2_dpc_payees				pay
	inner join
		scd2.scd2_dpc_contr_controls		cc
		on	pay.ocs_code	=	cc.ocs_code
	inner join
		scd2.scd2_dpc_payment_runs			pr
		on	pr.payment_run_id	=	cc.payment_run_id
		and	pr.schedule_id		=	cc.schedule_id
	/*	join to ILP for AUR prem fees	*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp320
		on  ilp320.payment_run_id = cc.payment_run_id
        and ilp320.schedule_id    = cc.schedule_id
        and ilp320.ocs_code       = cc.ocs_code
        and ilp320.dw_end_date    is null
        and ilp320.expense_head   = 320

	/*	join to ILP for AUR home fees	*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp322
		on  ilp320.payment_run_id = cc.payment_run_id
        and ilp320.schedule_id    = cc.schedule_id
        and ilp320.ocs_code       = cc.ocs_code
        and ilp320.dw_end_date    is null
        and ilp320.expense_head   = 322
	/*	join to ILP for Home delivery fees	- exclude M00178, M00248, M00249 due to previous data (reused expense head) and M00339 due to reuse for pharmacy CIP uplift*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp145
		on  ilp145.payment_run_id = cc.payment_run_id
        and ilp145.schedule_id    = cc.schedule_id
        and ilp145.ocs_code       = cc.ocs_code
        and ilp145.dw_end_date    is null
        and ilp145.expense_head   = 145
		and ilp145.payment_run_id not in ('M00178','M00248','M00249','M00339')
	/*	join to ILP for UMS Drugs	*/
		left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp164
		on  ilp164.payment_run_id = cc.payment_run_id
        and ilp164.schedule_id    = cc.schedule_id
        and ilp164.ocs_code       = cc.ocs_code
        and ilp164.dw_end_date    is null
        and ilp164.expense_head   = 164
	/*	join to ILP for UMS fees	*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp166
		on  ilp166.payment_run_id = cc.payment_run_id
        and ilp166.schedule_id    = cc.schedule_id
        and ilp166.ocs_code       = cc.ocs_code
        and ilp166.dw_end_date    is null
        and ilp166.expense_head   = 166
			/*	join to ILP for UMS charges	*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp168
		on  ilp168.payment_run_id = cc.payment_run_id
        and ilp168.schedule_id    = cc.schedule_id
        and ilp168.ocs_code       = cc.ocs_code
        and ilp168.dw_end_date    is null
        and ilp168.expense_head   = 168
		/*	join to ILP for CPCS drugs	*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp178
		on  ilp178.payment_run_id = cc.payment_run_id
        and ilp178.schedule_id    = cc.schedule_id
        and ilp178.ocs_code       = cc.ocs_code
        and ilp178.dw_end_date    is null
        and ilp178.expense_head   = 178
		/*	join to ILP for CSPC fees*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp180
		on  ilp180.payment_run_id = cc.payment_run_id
        and ilp180.schedule_id    = cc.schedule_id
        and ilp180.ocs_code       = cc.ocs_code
        and ilp180.dw_end_date    is null
        and ilp180.expense_head   = 180
		/*	join to ILP for CSPC charges	*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp182
		on  ilp180.payment_run_id = cc.payment_run_id
        and ilp180.schedule_id    = cc.schedule_id
        and ilp180.ocs_code       = cc.ocs_code
        and ilp180.dw_end_date    is null
        and ilp180.expense_head   = 182
		/*	join to ILP for CSPC set up*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp184
		on  ilp180.payment_run_id = cc.payment_run_id
        and ilp180.schedule_id    = cc.schedule_id
        and ilp180.ocs_code       = cc.ocs_code
        and ilp180.dw_end_date    is null
        and ilp180.expense_head   = 184
    /*	join to ILP for Hep C provision*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp193
		on  ilp193.payment_run_id = cc.payment_run_id
        and ilp193.schedule_id    = cc.schedule_id
        and ilp193.ocs_code       = cc.ocs_code
        and ilp193.dw_end_date    is null
        and
		ilp193.expense_head   = 193
      /*	join to ILP for Hep C reimbursement*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp195
		on  ilp195.payment_run_id = cc.payment_run_id
        and ilp195.schedule_id    = cc.schedule_id
        and ilp195.ocs_code       = cc.ocs_code
        and ilp195.dw_end_date    is null
        and ilp195.expense_head   = 195
 /*	join to ILP for PPE Claims*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp197
		on  ilp197.payment_run_id = cc.payment_run_id
        and ilp197.schedule_id    = cc.schedule_id
        and ilp197.ocs_code       = cc.ocs_code
        and ilp197.dw_end_date    is null
        and ilp197.expense_head   = 197
 /*	join to ILP for Discharge meds*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp201
		on  ilp201.payment_run_id = cc.payment_run_id
        and ilp201.schedule_id    = cc.schedule_id
        and ilp201.ocs_code       = cc.ocs_code
        and ilp201.dw_end_date    is null
        and ilp201.expense_head   = 201
/*	join to ILP for Covid-19 costs*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp208
		on  ilp208.payment_run_id = cc.payment_run_id
        and ilp208.schedule_id    = cc.schedule_id
        and ilp208.ocs_code       = cc.ocs_code
        and ilp208.dw_end_date    is null
        and ilp208.expense_head   = 208
/*	join to ILP for Covid test registration fee*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp211
		on  ilp211.payment_run_id = cc.payment_run_id
        and ilp211.schedule_id    = cc.schedule_id
        and ilp211.ocs_code       = cc.ocs_code
        and ilp211.dw_end_date    is null
        and ilp211.expense_head   = 211
/*	join to ILP for Covid test setup fee*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp212
		on  ilp212.payment_run_id = cc.payment_run_id
        and ilp212.schedule_id    = cc.schedule_id
        and ilp212.ocs_code       = cc.ocs_code
        and ilp212.dw_end_date    is null
        and ilp212.expense_head   = 212
/*	join to ILP for GP Referral Pathway Engagement Fee*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp213
		on  ilp213.payment_run_id = cc.payment_run_id
        and ilp213.schedule_id    = cc.schedule_id
        and ilp213.ocs_code       = cc.ocs_code
        and ilp213.dw_end_date    is null
        and ilp213.expense_head   = 213
/*	join to ILP for COVID Vaccine Claim*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp215
		on  ilp215.payment_run_id = cc.payment_run_id
        and ilp215.schedule_id    = cc.schedule_id
        and ilp215.ocs_code       = cc.ocs_code
        and ilp215.dw_end_date    is null
        and ilp215.expense_head   = 215
/*	join to ILP for COVID Premises and Refrigeration*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp217
		on  ilp217.payment_run_id = cc.payment_run_id
        and ilp217.schedule_id    = cc.schedule_id
        and ilp217.ocs_code       = cc.ocs_code
        and ilp217.dw_end_date    is null
        and ilp217.expense_head   = 217
/*	join to ILP for COVID Testing Kit*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp219
		on  ilp219.payment_run_id = cc.payment_run_id
        and ilp219.schedule_id    = cc.schedule_id
        and ilp219.ocs_code       = cc.ocs_code
        and ilp219.dw_end_date    is null
        and ilp219.expense_head   = 219
/*	join to ILP for CVD Hypertension set up fee*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp261
		on  ilp261.payment_run_id = cc.payment_run_id
        and ilp261.schedule_id    = cc.schedule_id
        and ilp261.ocs_code       = cc.ocs_code
        and ilp261.dw_end_date    is null
        and ilp261.expense_head   = 261
/*	join to ILP for CVD Hypertension check and ABPM fee*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp263
		on  ilp263.payment_run_id = cc.payment_run_id
        and ilp263.schedule_id    = cc.schedule_id
        and ilp263.ocs_code       = cc.ocs_code
        and ilp263.dw_end_date    is null
        and ilp263.expense_head   = 263
/*	join to ILP for CVD Hypertension Incentive*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp265
		on  ilp265.payment_run_id = cc.payment_run_id
        and ilp265.schedule_id    = cc.schedule_id
        and ilp265.ocs_code       = cc.ocs_code
        and ilp265.dw_end_date    is null
        and ilp265.expense_head   = 265
 /*	join to ILP for SCS Set-up*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp267
		on  ilp267.payment_run_id = cc.payment_run_id
        and ilp267.schedule_id    = cc.schedule_id
        and ilp267.ocs_code       = cc.ocs_code
        and ilp267.dw_end_date    is null
        and ilp267.expense_head   = 267
/*	join to ILP for SCS Consultations*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp269
		on  ilp269.payment_run_id = cc.payment_run_id
        and ilp269.schedule_id    = cc.schedule_id
        and ilp269.ocs_code       = cc.ocs_code
        and ilp269.dw_end_date    is null
        and ilp269.expense_head   = 269
 /*	join to ILP for SCS NRT Product Cost*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp271
		on  ilp271.payment_run_id = cc.payment_run_id
        and ilp271.schedule_id    = cc.schedule_id
        and ilp271.ocs_code       = cc.ocs_code
        and ilp271.dw_end_date    is null
        and ilp271.expense_head   = 271
 /*	join to ILP for SCS NRT Product Charges*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp273
		on  ilp273.payment_run_id = cc.payment_run_id
        and ilp273.schedule_id    = cc.schedule_id
        and ilp273.ocs_code       = cc.ocs_code
        and ilp273.dw_end_date    is null
        and ilp273.expense_head   = 273
/*	join to ILP for Tier 1 Contraception Set up*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp288
		on  ilp288.payment_run_id = cc.payment_run_id
        and ilp288.schedule_id    = cc.schedule_id
        and ilp288.ocs_code       = cc.ocs_code
        and ilp288.dw_end_date    is null
        and ilp288.expense_head   in (288,280)
/*	join to ILP for Tier 1 Contraception Product*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp284
		on  ilp284.payment_run_id = cc.payment_run_id
        and ilp284.schedule_id    = cc.schedule_id
        and ilp284.ocs_code       = cc.ocs_code
        and ilp284.dw_end_date    is null
        and ilp284.expense_head   = 284
/*	join to ILP for Tier 1 Contraception Consultation*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp282
		on  ilp282.payment_run_id = cc.payment_run_id
        and ilp282.schedule_id    = cc.schedule_id
        and ilp282.ocs_code       = cc.ocs_code
        and ilp282.dw_end_date    is null
        and ilp282.expense_head   = 282
/*	join to ILP for Pharmacy First Opt-in Fee/Pharmacy First Initial Fixed Payment*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp303
		on  ilp303.payment_run_id = cc.payment_run_id
        and ilp303.schedule_id    = cc.schedule_id
        and ilp303.ocs_code       = cc.ocs_code
        and ilp303.dw_end_date    is null
        and ilp303.expense_head   = 303
/*	join to ILP for Pharmacy First Consultation Fees*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp305
		on  ilp305.payment_run_id = cc.payment_run_id
        and ilp305.schedule_id    = cc.schedule_id
        and ilp305.ocs_code       = cc.ocs_code
        and ilp305.dw_end_date    is null
        and ilp305.expense_head   = 305
/*	join to ILP for Pharmacy First Payment*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp306
		on  ilp306.payment_run_id = cc.payment_run_id
        and ilp306.schedule_id    = cc.schedule_id
        and ilp306.ocs_code       = cc.ocs_code
        and ilp306.dw_end_date    is null
        and ilp306.expense_head   = 306
/*	join to ILP for PFCP Months*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp307
		on  ilp307.payment_run_id = cc.payment_run_id
        and ilp307.schedule_id    = cc.schedule_id
        and ilp307.ocs_code       = cc.ocs_code
        and ilp307.dw_end_date    is null
        and ilp307.expense_head   = 307
/*	join to ILP for PFCP VAT*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp308
		on  ilp308.payment_run_id = cc.payment_run_id
        and ilp308.schedule_id    = cc.schedule_id
        and ilp308.ocs_code       = cc.ocs_code
        and ilp308.dw_end_date    is null
        and ilp308.expense_head   = 308
/*	join to ILP for PF UMS & MI Charges Deducted*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp317
		on  ilp317.payment_run_id = cc.payment_run_id
        and ilp317.schedule_id    = cc.schedule_id
        and ilp317.ocs_code       = cc.ocs_code
        and ilp317.dw_end_date    is null
        and ilp317.expense_head   = 317
/*	join to ILP for PF UMS & MI Remuneration Payment*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp315
		on  ilp315.payment_run_id = cc.payment_run_id
        and ilp315.schedule_id    = cc.schedule_id
        and ilp315.ocs_code       = cc.ocs_code
        and ilp315.dw_end_date    is null
        and ilp315.expense_head   = 315
/*	join to ILP for PF UMS & MI Reimbursement Payment*/
	left outer join
		scd2.scd2_dpc_invoice_line_payments	ilp313
		on  ilp313.payment_run_id = cc.payment_run_id
        and ilp313.schedule_id    = cc.schedule_id
        and ilp313.ocs_code       = cc.ocs_code
        and ilp313.dw_end_date    is null
        and ilp313.expense_head   = 313
	inner join
		gps_tdim_202509							tdim
		on	pr.schedule_date	=	tdim.year_month
	where	1	=	1
		and	pay.active							in	('Y','R')
		and	substr(pay.ocs_code,1,1)			=	'F'
		and	pay.dw_end_date						is	null
		/*	fix for MIS bug	*/
		and	(
			cc.overall_total_amount				!=	0
			or	nvl(cc.tot_num_forms,0)			>=	1
			or	nvl(cc.local_ha_payments,0)		!=	0
			or	nvl(cc.local_ha_deductions,0)	!=	0
			)
		and	(cc.dw_end_date						is	null)
		and	pr.schedule_id						in	('PH','PL','AC') /* PH - normal pharmacy, PL - abated pharmacy, AC - appliance contractor	*/
		and	pr.dw_end_date						is	null
	group by
		pr.schedule_date
		,pay.ocs_code
)

select	*	from	dpc
;

/*	*/
/*	</CONSULT_FACT_DROP/>	*/
drop table 		gps_consult_fact_202509	cascade constraints	purge;
/*	</CONSULT_FACT_CREATE/>	*/
create table	gps_consult_fact_202509	compress for		query high	as

with

pf	as	(
	select
		tdim.financial_year
        ,fact.year_month
		,fact.disp_oupdt_type
		,fact.disp_id
		,fact.dispenser_code
		,sum(fact.item_count)					as	pf_items
		,sum(fact.item_pay_nic)/100				as	pf_nic


	from
		aml.px_form_item_elem_comb_fact_av	fact
	inner join
		gps_tdim_202509	tdim
		on	fact.year_month	=	tdim.year_month
	where	1	=	1
		and	fact.dispenser_country_ou	=	1
		and	fact.account_type			in	(5,8)
		--  regular exlusions
        and fact.PAY_DA_END         =   'N' -- excludes disallowed items
        and fact.PAY_ND_END         =   'N' -- excludes not dispensed items
        and fact.PAY_RB_END         =   'N' -- excludes referred back items
        and fact.CD_REQ             =   'N' -- excludes controlled drug requisitions
        and fact.OOHC_IND           =   0   -- excludes out of hours dispensing
        and fact.PRIVATE_IND        =   0   -- excludes private dispensers
        and fact.IGNORE_FLAG        =   'N' -- excludes LDP dummy forms
        --and fact.PRESC_TYPE_PRNT    not in  (8,54)  -- excludes private and pharmacy prescribers
		and nvl(fact.consult_only_ind, 'N') = 'N' -- excludes consultations only
        and fact.mys_service_type = 'CCS'
	group by
		tdim.financial_year
		,fact.year_month
		,fact.disp_oupdt_type
		,fact.disp_id
		,fact.dispenser_code

)
,
cont	as	(
	select
		tdim.financial_year
        ,fact.year_month
		,fact.disp_oupdt_type
		,fact.disp_id
		,fact.dispenser_code
		,sum(fact.item_count)					as	cont_items
		,sum(fact.item_pay_nic)/100				as	cont_nic


	from
		aml.px_form_item_elem_comb_fact_av	fact
	inner join
		gps_tdim_202509	tdim
		on	fact.year_month	=	tdim.year_month
	where	1	=	1
		and	fact.dispenser_country_ou	=	1
		and	fact.account_type			in	(5,8)
		--  regular exlusions
        and fact.PAY_DA_END         =   'N' -- excludes disallowed items
        and fact.PAY_ND_END         =   'N' -- excludes not dispensed items
        and fact.PAY_RB_END         =   'N' -- excludes referred back items
        and fact.CD_REQ             =   'N' -- excludes controlled drug requisitions
        and fact.OOHC_IND           =   0   -- excludes out of hours dispensing
        and fact.PRIVATE_IND        =   0   -- excludes private dispensers
        and fact.IGNORE_FLAG        =   'N' -- excludes LDP dummy forms
        --and fact.PRESC_TYPE_PRNT    not in  (8,54)  -- excludes private and pharmacy prescribers
		and nvl(fact.consult_only_ind, 'N') = 'N' -- excludes consultations only
        and fact.mys_service_type = 'CONT'
	group by
		tdim.financial_year
		,fact.year_month
		,fact.disp_oupdt_type
		,fact.disp_id
		,fact.dispenser_code

)
,
hyp	as	(
	select
		tdim.financial_year
        ,fact.year_month
		,fact.disp_oupdt_type
		,fact.disp_id
		,fact.dispenser_code
		,sum(fact.item_count)					as	hyp_items
		,sum(fact.item_pay_nic)/100				as	hyp_nic


	from
		aml.px_form_item_elem_comb_fact_av	fact
	inner join
		gps_tdim_202509	tdim
		on	fact.year_month	=	tdim.year_month
	where	1	=	1
		and	fact.dispenser_country_ou	=	1
		and	fact.account_type			in	(5,8)
		--  regular exlusions
        and fact.PAY_DA_END         =   'N' -- excludes disallowed items
        and fact.PAY_ND_END         =   'N' -- excludes not dispensed items
        and fact.PAY_RB_END         =   'N' -- excludes referred back items
        and fact.CD_REQ             =   'N' -- excludes controlled drug requisitions
        and fact.OOHC_IND           =   0   -- excludes out of hours dispensing
        and fact.PRIVATE_IND        =   0   -- excludes private dispensers
        and fact.IGNORE_FLAG        =   'N' -- excludes LDP dummy forms
        --and fact.PRESC_TYPE_PRNT    not in  (8,54)  -- excludes private and pharmacy prescribers
		and nvl(fact.consult_only_ind, 'N') = 'N' -- excludes consultations only
        and fact.mys_service_type = 'HYP'
	group by
		tdim.financial_year
		,fact.year_month
		,fact.disp_oupdt_type
		,fact.disp_id
		,fact.dispenser_code

)

select
pf.financial_year
        ,pf.year_month
		,pf.disp_oupdt_type
		,pf.disp_id
		,pf.dispenser_code
        ,pf.pf_items
        ,pf.pf_nic
        ,cont.cont_items
        ,cont.cont_nic
        ,hyp.hyp_items
        ,hyp.hyp_nic

        from	pf
left outer join
cont
		on	pf.year_month	=	cont.year_month
        and pf.disp_oupdt_type = cont.disp_oupdt_type
		and pf.disp_id = cont.disp_id
		and pf.dispenser_code = cont.dispenser_code
left outer join
hyp
		on	pf.year_month	=	hyp.year_month
        and pf.disp_oupdt_type = hyp.disp_oupdt_type
		and pf.disp_id = hyp.disp_id
		and pf.dispenser_code = hyp.dispenser_code
;
