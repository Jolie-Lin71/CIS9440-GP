
WITH foodraw AS(
SELECT * 
FROM cis9440gp.RawDataset.FoodEstablishment
),
foodraw_adddateid AS(
SELECT FORMAT_DATE('%Y%m%d', DATE(created_date)) AS date_id,
        *
FROM foodraw
ORDER BY date_id
)
SELECT unique_key, date_id
FROM foodraw_adddateid f
INNER JOIN cis9440gp.dbt_qlin.date_dimension d
ON f.date_id = d.date_dim_id