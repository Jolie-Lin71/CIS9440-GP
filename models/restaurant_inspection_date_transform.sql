
WITH resraw AS(
SELECT * 
FROM cis9440gp.RawDataset.RestaurantInspection
),
resraw_adddateid AS(
SELECT FORMAT_DATE('%Y%m%d', DATE(inspection_date)) AS date_id,
        *
FROM resraw
ORDER BY date_id
)
SELECT  camis, date_id
FROM resraw_adddateid r
INNER JOIN cis9440gp.dbt_qlin.date_dimension d
ON r.date_id = d.date_dim_id