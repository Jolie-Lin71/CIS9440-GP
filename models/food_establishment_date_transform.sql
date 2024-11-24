
WITH foodraw AS(
SELECT * 
FROM cis9440gp.RawData.FoodEstablishment
)

SELECT FORMAT_DATE('%Y%m%d', DATE(created_date)) AS date_id,
        *
FROM foodraw