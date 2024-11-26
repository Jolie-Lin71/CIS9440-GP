
-- models/complaints_fact_table.sql
{{ config(materialized="table") }}

WITH complaints_fact_table AS (
    SELECT
        unique_key,
        ctd.complaint_type_dim_id,
        dd.date_dim_id,
        jd.junk_dim_id
    FROM `cis9440gp.RawDataset.FoodEstablishment` complaints
    LEFT JOIN `cis9440gp.dbt_cliu.complaint_type_dimension` ctd
        ON complaints.descriptor = ctd.complaint_descriptor
    LEFT JOIN `cis9440gp.dbt_qlin.date_dimension` dd
        ON DATE(complaints.created_date) = dd.date
    LEFT JOIN `cis9440gp.dbt_cliu.junk_dimension` jd
        ON CASE
            WHEN complaints.location_type IN ('Cafeteria - College/University', 'Cafeteria - Private School', 'Cafeteria - Public School') THEN 'Educational Cafeteria'
            WHEN complaints.location_type IN ('Food Cart Vendor', 'Mobile Food Vendor', 'Street Vendor', 'Street Fair Vendor') THEN 'Mobile Food Service'
            WHEN complaints.location_type IN ('Restaurant', 'Restaurant/Bar/Deli/Bakery') THEN 'Restaurant Types'
            WHEN complaints.location_type IN ('Catering Service', 'Catering Hall') THEN 'Catering Operations'
            ELSE 'Other Types'
           END = jd.food_establishment_type_category
           AND jd.resolution_status=complaints.status
)
SELECT *
FROM complaints_fact_table
/*
where unique_key='62407632'
--verify junk dimension
select `descriptor`,created_date, location_type, status
from `cis9440gp.RawDataset.FoodEstablishment`
where unique_key='62407632'
*/