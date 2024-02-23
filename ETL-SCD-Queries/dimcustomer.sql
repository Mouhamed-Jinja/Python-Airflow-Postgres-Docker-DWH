SELECT customerid, country, processed_date
FROM (
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY customerid ORDER BY processed_date DESC) AS rn
    FROM (
        SELECT customerid, country, processed_date 
        FROM dimcustomer
        
        UNION ALL
        
        SELECT customerid, country, CURRENT_DATE + INTERVAL '1 day' AS processed_date 
        FROM retail_cleaned
    ) AS combined
) AS nt
WHERE rn = 1

