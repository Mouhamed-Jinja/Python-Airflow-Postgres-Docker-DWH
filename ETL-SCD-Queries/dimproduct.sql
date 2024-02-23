SELECT stockcode, description, processed_date
FROM (
    SELECT stockcode, description, processed_date,
           ROW_NUMBER() OVER (PARTITION BY stockcode ORDER BY processed_date DESC) AS row_num
    FROM (
        SELECT stockcode,
               description,
               CURRENT_DATE + INTERVAL '1 day' AS processed_date
        FROM retail_cleaned --new records

        UNION ALL

        SELECT stockcode,
               description,
               processed_date
        FROM dimproduct
    ) AS combined
) AS rn
WHERE row_num = 1