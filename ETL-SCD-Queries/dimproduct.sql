SELECT stockcode, description
FROM (
    SELECT stockcode, description, processed_date,
           ROW_NUMBER() OVER (PARTITION BY stockcode ORDER BY processed_date DESC) AS row_num
    FROM (
        SELECT stockcode,
               description,
               CURRENT_DATE AS processed_date
        FROM retail_cleaned --new records

        UNION ALL

        SELECT stockcode,
               description,
               processed_date
        FROM dimproduct
    ) AS combined
) AS rn
WHERE row_num = 1

UNION ALL

SELECT stockcode, description
FROM retail_cleaned
WHERE stockcode NOT IN (SELECT stockcode FROM dimproduct);