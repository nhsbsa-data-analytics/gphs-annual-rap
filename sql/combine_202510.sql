/*	<TITLE>	gps-combine-202510.sql

	<DESCRIPTION>	joins data from current file and blank file

	<DETAILS>	dpc code can not be ran for more than 7 years data due to financial rules therefore anything prior to 2018 will be inaccurate for dpc fields and must be taken from previously run data

	<CREATED>	14/10/2025

	<CREATED BY>	 KIGRA
blank is based on gps_final_202409_combined
done to account for differences in pd1 rebuild for previous years after QR

*/
CREATE TABLE gps_final_202510_combined as
SELECT * FROM GPS_FINAL_202509_BLANK
WHERE FINANCIAL_YEAR in ('2015/2016','2016/2017','2017/2018','2018/2019','2019/2020','2020/2021','2021/2022','2022/2023')
UNION ALL
SELECT * FROM gps_final_202509
WHERE FINANCIAL_YEAR not in ('2015/2016','2016/2017','2017/2018','2018/2019','2019/2020','2020/2021','2021/2022','2022/2023');
