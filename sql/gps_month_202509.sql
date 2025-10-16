/*	<TITLE>	gps-month-202509.sql

	<DESCRIPTION>	outputs data for the General Pharmaceutical Services National Statistic publication

	<DETAILS>	code joins the individual staging tables for GPS together to give one final output tables
				at monthly and account level.

	<CREATED>	06/10/2022

	<CREATED BY>	 KIGRA

	<AMENDED> 19/09/2023 KIGRA for 2022/23 release
				added tier 1 contraception set up (both expense heads), tier 1 contraception product cost, tier 1 contraception consultations
				27/09/2023
				added rounding to average pd1 items to prevent data loss with between
	<AMENDED> 18/09/2024 KIGRA for 2023/24 release
				added Pharmacy first intial fees, consultation fees, payment, months, vat, umsmi deductions, umsmi remuneration and umsmi reimbursent

*/

/*	final select that joins all staging tables together	*/
drop table gps_month_202509;
create table gps_month_202509 as
select
	pd1.year_month
    ,case
when pd1.year_month =  '202104' then 'Apr-21'
when pd1.year_month =  '202105' then 'May-21'
when pd1.year_month =  '202106' then 'Jun-21'
when pd1.year_month =  '202107' then 'Jul-21'
when pd1.year_month =  '202108' then 'Aug-21'
when pd1.year_month =  '202109' then 'Sep-21'
when pd1.year_month =  '202110' then 'Oct-21'
when pd1.year_month =  '202111' then 'Nov-21'
when pd1.year_month =  '202112' then 'Dec-21'
when pd1.year_month =  '202201' then 'Jan-22'
when pd1.year_month =  '202202' then 'Feb-22'
when pd1.year_month =  '202203' then 'Mar-22'
when pd1.year_month =  '202204' then 'Apr-22'
when pd1.year_month =  '202205' then 'May-22'
when pd1.year_month =  '202206' then 'Jun-22'
when pd1.year_month =  '202207' then 'Jul-22'
when pd1.year_month =  '202208' then 'Aug-22'
when pd1.year_month =  '202209' then 'Sep-22'
when pd1.year_month =  '202210' then 'Oct-22'
when pd1.year_month =  '202211' then 'Nov-22'
when pd1.year_month =  '202212' then 'Dec-22'
when pd1.year_month =  '202301' then 'Jan-23'
when pd1.year_month =  '202302' then 'Feb-23'
when pd1.year_month =  '202303' then 'Mar-23'
when pd1.year_month =  '202304' then 'Apr-23'
when pd1.year_month =  '202305' then 'May-23'
when pd1.year_month =  '202306' then 'Jun-23'
when pd1.year_month =  '202307' then 'Jul-23'
when pd1.year_month =  '202308' then 'Aug-23'
when pd1.year_month =  '202309' then 'Sep-23'
when pd1.year_month =  '202310' then 'Oct-23'
when pd1.year_month =  '202311' then 'Nov-23'
when pd1.year_month =  '202312' then 'Dec-23'
when pd1.year_month =  '202401' then 'Jan-24'
when pd1.year_month =  '202402' then 'Feb-24'
when pd1.year_month =  '202403' then 'Mar-24'
when pd1.year_month =  '202404' then 'Apr-24'
when pd1.year_month =  '202405' then 'May-24'
when pd1.year_month =  '202406' then 'Jun-24'
when pd1.year_month =  '202407' then 'Jul-24'
when pd1.year_month =  '202408' then 'Aug-24'
when pd1.year_month =  '202409' then 'Sep-24'
when pd1.year_month =  '202410' then 'Oct-24'
when pd1.year_month =  '202411' then 'Nov-24'
when pd1.year_month =  '202412' then 'Dec-24'
when pd1.year_month =  '202501' then 'Jan-25'
when pd1.year_month =  '202502' then 'Feb-25'
when pd1.year_month =  '202503' then 'Mar-25'
else null end as pd1_month
    ,dorg.region_name
	,dorg.region_code
	,dorg.icb_name
	,dorg.icb_code
	,dorg.dispenser_code
	,dorg.appliance_dispenser_hist
	,dorg.lps_dispenser_hist
	,dorg.dist_selling_dispenser_hist
	,dorg.contractor_type
	,dorg.close_date_ltst
	,dorg.close_date_hist
	,dorg.start_date_hist
	,pd1.month_count
	,sum(pd1.pd1_items)						as	items
	,sum(pd1.pd1_items)/pd1.month_count		as	avg_monthly_items
	,case	when	round(sum(pd1.pd1_items)/pd1.month_count,0)		between	0		and	2000	then	'0 - 2000'
			when	round(sum(pd1.pd1_items)/pd1.month_count,0)		between	2001	and	4000	then	'2001 - 4000'
			when	round(sum(pd1.pd1_items)/pd1.month_count,0)		between	4001	and	6000	then	'4001 - 6000'
			when	round(sum(pd1.pd1_items)/pd1.month_count,0)		between	6001	and	8000	then	'6001 - 8000'
			when	round(sum(pd1.pd1_items)/pd1.month_count,0)		between	8001	and	10000	then	'8001 - 10000'
			when	round(sum(pd1.pd1_items)/pd1.month_count,0)		>		10000				then	'10000+'
			else	'N/A'
	end	as	monthly_disp_vol_band
	,sum(pd1.pd1_nic)						as	nic
	,sum(pd1.pd1_prof_fees)					as	prof_fees
	,sum(pd1.pd1_num_prof_fees)				as	num_prof_fees
	,sum(pd1.pd1_mur_fees)					as	mur_fees
	,sum(pd1.pd1_num_mur)					as	num_mur
	,sum(pd1.pd1_nms_fees)					as	nms_fees
	,sum(pd1.pd1_num_nms)					as	num_nms
	,sum(pd1.pd1_aur_home)					as	aur_home
	,sum(pd1.pd1_aur_prem)					as	aur_prem
	,sum(pd1.pd1_cd_fee) 					as cd_fee
	,sum(pd1.pd1_cd_sched2_fee)				as cd_sched2_fee
	,sum(pd1.pd1_cd_sched3_fee)				as cd_sched3_fee
	,sum(pd1.pd1_add_fee_2a) 				as add_fee_2a
	,sum(pd1.pd1_add_fee_mf) 				as add_fee_mf
	,sum(pd1.pd1_expensive_fee) 			as expensive_fee
	,sum(pd1.pd1_expensive_px_fees) 		as no_expensive_fees
	,sum(pd1.pd1_oope_item_count) 			as oope_item_count
	,sum(pd1.pd1_oope_val) 					as oope_val
	,sum(pd1.pd1_ssp_fees) 				as ssp_fees
	,sum(fact.fact_items)					as	check_items
	,sum(fact.fact_dr_nic)					as	check_dr_nic
	,sum(fact.fact_nic)						as	check_nic
	,sum(fact.fact_num_prof_fees)			as	check_num_prof_fees
	,sum(fact.fact_prof_fees)				as	check_prof_fees
	,sum(fact.eps_items)					as	eps_items
	,sum(fact.custom_stoma_fees)			as	custom_stoma_fees
	,sum(fact.num_custom_stoma_fees)		as	num_custom_stoma_fees
	,sum(fact.item_custom_stoma_fees)		as	item_custom_stoma_fees
	,sum(fact.item_num_custom_stoma_fees)	as	item_num_custom_stoma_fees
	,sum(nvl(flu.flu_items,0))				as	flu_items
	,sum(nvl(flu.flu_cost,0))				as	flu_cost
	,sum(nvl(flu.flu_fees,0))				as	flu_fees
	,sum(dpc.dpc_num_aur_home)				as	num_aur_home
	,sum(dpc.dpc_num_aur_prem)				as	num_aur_prem
	,sum(dpc.dpc_aur_home)					as	check_aur_home
	,sum(dpc.dpc_aur_prem)					as	check_aur_prem
	,sum(dpc.dpc_num_home_del) 				as  num_home_del
	,sum(dpc.dpc_home_del) 					as  home_del_cost
	,sum(dpc.dpc_ums_drugs) 				as ums_drugs
	,sum(dpc.dpc_ums_fees) 					as ums_fees
	,sum(dpc.dpc_ums_charges) 				as ums_charges
	,sum(dpc.dpc_cpcs_drugs) 				as cpcs_drugs
	,sum(dpc.dpc_cpcs_fees) 				as cpcs_fees
	,sum(dpc.dpc_cpcs_charges) 				as cpcs_charges
	,sum(dpc.dpc_cpcs_signup) 				as cpcs_signup
	,sum(dpc.dpc_hep_c_service) 			as hep_c_service
	,sum(dpc.dpc_hep_c_kit) 				as hep_c_kit
	,sum(dpc.dpc_ppe_claims) 				as ppe_claims
	,sum(dpc.dpc_discharge_meds) 			as discharge_meds
	,sum(dpc.dpc_cvd_19_costs) 				as cvd_19_costs
	,sum(dpc.dpc_cvdtestreg_fees) 			as cvdtestreg_fees
	,sum(dpc.dpc_cvdtestset_fees) 			as cvdtestset_fees
	,sum(dpc.dpc_gprefpath_fees) 			as gprefpath_fees
	,sum(dpc.dpc_cvd_19_vaccine) 			as cvd_19_vaccine
	,sum(dpc.dpc_cvd_prem_refrid) 			as cvd_prem_refrid
	,sum(dpc.dpc_cvdtest_kits) 				as cvdtest_kits
	,sum(dpc.dpc_hyptenset_fees) 			as hyptenset_fees
	,sum(dpc.dpc_hyptencheck_fees) 			as hyptencheck_fees
	,sum(dpc.dpc_hypten_inc) 				as hypten_inc
	,sum(dpc.dpc_scs_setup) 				as scs_setup
	,sum(dpc.dpc_scs_cnslt) 				as scs_cnslt
	,sum(dpc.dpc_scs_nrtprod_cost) 			as scs_nrtprod_cost
	,sum(dpc.dpc_scs_nrtprod_charge) 		as scs_nrtprod_charge
	,sum(dpc.dpc_t1c_set_up) 				as t1c_set_up
	,sum(dpc.dpc_t1c_prod_cost) 			as t1c_prod_cost
	,sum(dpc.dpc_t1c_consult) 				as t1c_consult
	,sum(dpc.dpc_pfcp_optin)                as pfcp_optin
	,sum(dpc.dpc_pfcp_fees) 	        	as pfcp_fees
	,sum(dpc.dpc_pfcp_payment) 		        as pfcp_payment
	,sum(dpc.dpc_pfcp_months) 		        as pfcp_months
	,sum(dpc.dpc_pfcp_vat) 		            as pfcp_vat
	,sum(dpc_pfcp_umsmideduct) 		        as pfcp_umsmideduct
	,sum(dpc.dpc_pfcp_umsmiremuneration) 	as pfcp_umsmiremuneration
	,sum(dpc.dpc_pfcp_umsmireimbursement) 	as pfcp_umsmireimbursement
	,sum(consult.pf_items)                  as pf_items
	,sum(consult.pf_nic)                    as pf_nic
	,sum(consult.cont_items)                  as cont_items
	,sum(consult.cont_nic)                    as cont_nic
	,sum(consult.hyp_items)                  as hyp_items
	,sum(consult.hyp_nic)                    as hyp_nic

from
	gps_pd1_202509		pd1
inner join
	gps_fact_202509	fact
	on	pd1.disp_oupdt_type	=	fact.disp_oupdt_type
	and	pd1.disp_id			=	fact.disp_id
	and	pd1.year_month		=	fact.year_month
left outer join
	gps_flu_202509		flu
	on	pd1.disp_oupdt_type	=	flu.level_5_oupdt
	and	pd1.disp_id			=	flu.level_5_ou
	and	pd1.year_month		=	flu.year_month
inner join
	gps_dorg_202509	dorg
	on	pd1.disp_oupdt_type	=	dorg.lvl_5_oupdt
	and	pd1.disp_id			=	dorg.lvl_5_ou
	and	pd1.year_month		=	dorg.year_month
left outer join
	gps_dpc_202509		dpc
	on	dorg.dispenser_code	=	dpc.dispenser_code
	and	dorg.year_month		=	dpc.year_month
left outer join
	gps_consult_fact_202509		consult
	on	pd1.disp_oupdt_type	=	consult.disp_oupdt_type
	and	pd1.disp_id			=	consult.disp_id
	and	pd1.year_month		=	consult.year_month
where	1	=	1
and pd1.financial_year in('2021/2022', '2022/2023','2023/2024','2024/2025')
group by
	pd1.year_month
	,dorg.region_name
	,dorg.region_code
	,dorg.icb_name
	,dorg.icb_code
	,dorg.dispenser_code
	,dorg.appliance_dispenser_hist
	,dorg.lps_dispenser_hist
	,dorg.dist_selling_dispenser_hist
	,dorg.contractor_type
	,dorg.close_date_ltst
	,dorg.close_date_hist
	,dorg.start_date_hist
	,pd1.month_count
order by
	pd1.year_month
	,dispenser_code
