/*	<TITLE>	gps-combine-202509.sql

	<DESCRIPTION>	joins data from current file and previous combined file

	<DETAILS>	dpc code can not be ran for more than 7 years data due to financial rules therefore anything prior to 2018 will be inaccurate for dpc fields and must be taken from previously run data

	<CREATED>	06/10/2023

	<CREATED BY>	 KIGRA

    <AMENDED> 20/08/2025 to add 2018/2019 filter
    <AMENDED> to add in items and nic from pharmacy first as blanks to previous data before combine
*/
drop table gps_final_202509_blank;
CREATE TABLE gps_final_202509_blank as
select *
from
gps_final_202409_combined;
ALTER TABLE gps_final_202509_blank
ADD pf_items number NULL
ADD pf_nic number NULL
ADD cont_items number NULL
ADD cont_nic number NULL
ADD hyp_items number NULL
ADD hyp_nic number NULL

drop table gps_final_202509_combined;
CREATE TABLE gps_final_2024509_combined as
SELECT * FROM gps_final_202309_blank
WHERE FINANCIAL_YEAR in ('2015/2016','2016/2017','2017/2018','2018/2019')
UNION ALL
SELECT * FROM gps_final_202509
WHERE FINANCIAL_YEAR not in ('2015/2016','2016/2017','2017/2018','2018/2019');
