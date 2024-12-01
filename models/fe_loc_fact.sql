{{
  config(
    materialized='table'
  )
}}

WITH base_fe AS(
    SELECT *
    FROM `cis9440gp.RawDataset.FoodEstablishment`
),
location_fe AS(
    SELECT unique_key, borough, community_board, incident_zip, street_name
    FROM base_fe
),
loc_fe_transformed AS(
    SELECT unique_key,
        borough AS city_borough,
        CASE 
                WHEN community_board='01 MANHATTAN' THEN '101'
                WHEN community_board='02 MANHATTAN' THEN '102'
                WHEN community_board='03 MANHATTAN' THEN '103'
                WHEN community_board='04 MANHATTAN' THEN '104'
                WHEN community_board='05 MANHATTAN' THEN '105'
                WHEN community_board='06 MANHATTAN' THEN '106'
                WHEN community_board='07 MANHATTAN' THEN '107'
                WHEN community_board='08 MANHATTAN' THEN '108'
                WHEN community_board='09 MANHATTAN' THEN '109'
                WHEN community_board='10 MANHATTAN' THEN '110'
                WHEN community_board='11 MANHATTAN' THEN '111'
                WHEN community_board='12 MANHATTAN' THEN '112'
                WHEN community_board='Unspecified MANHATTAN' THEN '100'
                WHEN community_board='01 BRONX' THEN '201'
                WHEN community_board='02 BRONX' THEN '202'
                WHEN community_board='03 BRONX' THEN '203'
                WHEN community_board='04 BRONX' THEN '204'
                WHEN community_board='05 BRONX' THEN '205'
                WHEN community_board='06 BRONX' THEN '206'
                WHEN community_board='07 BRONX' THEN '207'
                WHEN community_board='08 BRONX' THEN '208'
                WHEN community_board='09 BRONX' THEN '209'
                WHEN community_board='10 BRONX' THEN '210'
                WHEN community_board='11 BRONX' THEN '211'
                WHEN community_board='12 BRONX' THEN '212'
                WHEN community_board='Unspecified BRONX' THEN '200'
                WHEN community_board='01 BROOKLYN' THEN '301'
                WHEN community_board='02 BROOKLYN' THEN '302'
                WHEN community_board='03 BROOKLYN' THEN '303'
                WHEN community_board='04 BROOKLYN' THEN '304'
                WHEN community_board='05 BROOKLYN' THEN '305'
                WHEN community_board='06 BROOKLYN' THEN '306'
                WHEN community_board='07 BROOKLYN' THEN '307'
                WHEN community_board='08 BROOKLYN' THEN '308'
                WHEN community_board='09 BROOKLYN' THEN '309'
                WHEN community_board='10 BROOKLYN' THEN '310'
                WHEN community_board='11 BROOKLYN' THEN '311'
                WHEN community_board='12 BROOKLYN' THEN '312'
                WHEN community_board='13 BROOKLYN' THEN '313'
                WHEN community_board='14 BROOKLYN' THEN '314'
                WHEN community_board='15 BROOKLYN' THEN '315'
                WHEN community_board='16 BROOKLYN' THEN '316'
                WHEN community_board='17 BROOKLYN' THEN '317'
                WHEN community_board='18 BROOKLYN' THEN '318'
                WHEN community_board='Unspecified BROOKLYN' THEN '300' 
                WHEN community_board='01 QUEENS' THEN '401'
                WHEN community_board='02 QUEENS' THEN '402'
                WHEN community_board='03 QUEENS' THEN '403'
                WHEN community_board='04 QUEENS' THEN '404'
                WHEN community_board='05 QUEENS' THEN '405'
                WHEN community_board='06 QUEENS' THEN '406'
                WHEN community_board='07 QUEENS' THEN '407'
                WHEN community_board='08 QUEENS' THEN '408'
                WHEN community_board='09 QUEENS' THEN '409'
                WHEN community_board='10 QUEENS' THEN '410'
                WHEN community_board='11 QUEENS' THEN '411'
                WHEN community_board='12 QUEENS' THEN '412'
                WHEN community_board='13 QUEENS' THEN '413'
                WHEN community_board='14 QUEENS' THEN '414'
                WHEN community_board='Unspecified QUEENS' THEN '400' 
                WHEN community_board='01 STATEN ISLAND' THEN '501'
                WHEN community_board='02 STATEN ISLAND' THEN '502'
                WHEN community_board='03 STATEN ISLAND' THEN '503'
                WHEN community_board='Unspecified STATEN ISLAND' THEN '500'
                ELSE '999'
            END AS community_board,
            COALESCE(incident_zip,'99999') AS zipcode,
            COALESCE(street_name,'Unspecified') AS street_address

    FROM location_fe
)
SELECT unique_key, location_dim_id
FROM loc_fe_transformed r
LEFT JOIN `cis9440gp.dbt_qlin.location_dimension` d
ON r.city_borough=d.city_borough
AND r.community_board=d.community_board
AND r.zipcode=d.zipcode
AND r.street_address=d.street_address



/*
--check if there is unique_key in rawdataset withought corresponding location_dim_id
,
final as (
SELECT unique_key, location_dim_id
FROM loc_fe_transformed r
LEFT JOIN {{ ref('location_dimension') }} d
ON r.city_borough=d.city_borough
AND r.community_board=d.community_board
AND r.zipcode=d.zipcode
AND r.street_address=d.street_address
)
select location_dim_id
from final
where location_dim_id is null
*/