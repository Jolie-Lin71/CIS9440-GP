{{
  config(
    materialized='table'
  )
}}

with basedates as(
    {{ dbt_date.get_base_dates(start_date="2019-01-01", end_date="2024-09-30", datepart="day") }}
),
date_only as (
    SELECT DATE(date_day) AS `date`
    FROM basedates
),
date_yqmw as(  
    SELECT `date`,
    EXTRACT(YEAR FROM `date`) AS `year`,
    EXTRACT(QUARTER FROM `date`) AS `quarter`,
    EXTRACT(MONTH FROM `date`) AS `month`,
    EXTRACT(DAY FROM `date`) AS `day`,
    EXTRACT(DAYOFWEEK FROM `date`) AS day_of_week
    FROM date_only
),
date_yqmw_name as(
    SELECT `date`,
    CASE 
        WHEN `quarter` = 1 THEN 'Q1'
        WHEN `quarter` = 2 THEN 'Q2'
        WHEN `quarter` = 3 THEN 'Q3'
        WHEN `quarter` = 4 THEN 'Q4'
    END AS quarter_name,
    CASE
        WHEN `month` = 1 THEN 'January'
        WHEN `month` = 2 THEN 'February'
        WHEN `month` = 3 THEN 'March'
        WHEN `month` = 4 THEN 'April'
        WHEN `month` = 5 THEN 'May'
        WHEN `month` = 6 THEN 'June'
        WHEN `month` = 7 THEN 'July'
        WHEN `month` = 8 THEN 'August'
        WHEN `month` = 9 THEN 'September'
        WHEN `month` = 10 THEN 'October'
        WHEN `month` = 11 THEN 'November'
        WHEN `month` = 12 THEN 'December'
    END AS month_name,
    CASE
        WHEN day_of_week = 1 THEN 'Sunday'
        WHEN day_of_week = 2 THEN 'Monday'
        WHEN day_of_week = 3 THEN 'Tuesday'
        WHEN day_of_week = 4 THEN 'Wednesday'
        WHEN day_of_week = 5 THEN 'Thursday'
        WHEN day_of_week = 6 THEN 'Friday'
        WHEN day_of_week = 7 THEN 'Saturday'
    END AS day_of_week_name,
    CASE
        WHEN `date` < '2020-03-13' THEN 'before-COVID'
        WHEN `date` BETWEEN '2020-03-13' AND '2023-05-11' THEN 'COVID'
        WHEN `date` > '2023-05-11' THEN 'post-COVID'
    END AS covid_status
    FROM date_yqmw
),
date_halfway as(
SELECT `date`,`year`,`quarter`,`month`,`day`,day_of_week,
        quarter_name,month_name,day_of_week_name,covid_status
FROM date_yqmw_name
INNER JOIN date_yqmw
USING (`date`)
)
SELECT FORMAT_DATE('%Y%m%d',`date`) AS date_dim_id,
       *
FROM date_halfway