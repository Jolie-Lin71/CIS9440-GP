{{
  config(
    materialized='table'
  )
}}


WITH base_ri AS(
    SELECT *
    FROM `cis9440gp.RawDataset.RestaurantInspection` 
),
location_ri AS(
    SELECT camis, boro, community_board, zipcode, street
    FROM base_ri
),
loc_ri_transformed AS (
    SELECT  camis,
            UPPER(boro) AS city_borough,
            CASE 
                WHEN community_board ='164' THEN '100'
                WHEN community_board like '22%' THEN '200'
                WHEN community_board like '355' THEN '300'
                WHEN community_board like '48%' THEN '400'
                WHEN community_board='595' THEN '500'
                WHEN boro ='Manhattan' AND community_board IS NULL THEN '100'
                WHEN boro ='Bronx' AND community_board IS NULL THEN '200'
                WHEN boro ='Brooklyn' AND community_board IS NULL THEN '300'
                WHEN boro ='Queens' AND community_board IS NULL THEN '400'
                WHEN boro ='Staten Island' AND community_board IS NULL THEN '500'
                ELSE community_board 
            END AS community_board,
            COALESCE(zipcode,'99999') AS zipcode,
            COALESCE(street,'Unspecified') AS street_address
    FROM location_ri
)
SELECT camis, location_dim_id
FROM loc_ri_transformed r
LEFT JOIN `cis9440gp.dbt_qlin.location_dimension` d
ON r.city_borough=d.city_borough
AND r.community_board=d.community_board
AND r.zipcode=d.zipcode
AND r.street_address=d.street_address


/*
--check if there is camis in rawdataset withought corresponding location_dim_id
, 
final as(
SELECT camis, location_dim_id
FROM loc_ri_transformed r
LEFT JOIN `cis9440gp.dbt_qlin.location_dimension` d
ON r.city_borough=d.city_borough
AND r.community_board=d.community_board
AND r.zipcode=d.zipcode
AND r.street_address=d.street_address
)
select location_dim_id
from final
where location_dim_id is null
*/