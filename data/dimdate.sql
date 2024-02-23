WITH old_max AS (
    SELECT MAX(invoicedate) AS max_date FROM dimdate
),
new_max AS (
    SELECT MAX(invoicedate) AS max_date FROM retail_cleaned
)

SELECT 
    generate_series((old_max.max_date + INTERVAL '1 DAY'), new_max.max_date, INTERVAL '1 DAY')::TIMESTAMP AS datekey,
    generate_series((old_max.max_date + INTERVAL '1 DAY'), new_max.max_date, INTERVAL '1 DAY')::TIMESTAMP AS invoicedate,
    TO_CHAR(generate_series((old_max.max_date + INTERVAL '1 DAY'), new_max.max_date, INTERVAL '1 DAY')::DATE, 'YYYY-MM-DD') AS date,
    TO_CHAR(generate_series((old_max.max_date + INTERVAL '1 DAY'), new_max.max_date, INTERVAL '1 DAY')::DATE, 'YYYY') AS year,
    TO_CHAR(generate_series((old_max.max_date + INTERVAL '1 DAY'), new_max.max_date, INTERVAL '1 DAY')::DATE, 'MM') AS monthno,
    TO_CHAR(generate_series((old_max.max_date + INTERVAL '1 DAY'), new_max.max_date, INTERVAL '1 DAY')::DATE, 'Month') AS monthname,
    TO_CHAR(generate_series((old_max.max_date + INTERVAL '1 DAY'), new_max.max_date, INTERVAL '1 DAY')::DATE, 'DD') AS day,
    EXTRACT(QUARTER FROM generate_series((old_max.max_date + INTERVAL '1 DAY'), new_max.max_date, INTERVAL '1 DAY')::DATE) AS quarter
FROM 
    old_max, new_max
WHERE 
    new_max.max_date > old_max.max_date;
