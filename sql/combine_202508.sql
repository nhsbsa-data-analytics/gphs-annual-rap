/*	<TITLE>	gps-combine-202508.sql

	<DESCRIPTION>	joins data from current file and previous combined file

	<DETAILS>	dpc code can not be ran for more than 7 years data due to financial rules therefore anything prior to 2018 will be inaccurate for dpc fields and must be taken from previously run data

	<CREATED>	06/10/2023

	<CREATED BY>	 KIGRA

    <AMENDED> 20/08/2025 to add 2018/2019 filter 
*/
drop table gps_final_202508_combined;
CREATE TABLE gps_final_202508_combined as
SELECT * FROM gps_final_202508_blank 
WHERE FINANCIAL_YEAR in ('2015/2016','2016/2017','2017/2018','2018/2019')
UNION ALL 
SELECT * FROM gps_final_202409_combined
WHERE FINANCIAL_YEAR not in ('2015/2016','2016/2017','2017/2018','2018/2019');