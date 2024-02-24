
--  saleskey invoiceno datekey  customerkey  productkey  unitprice  quantity
SELECT r.invoiceno,
    r.invoicedate as datekey,
    r.invoicedate,
    r.customerid as customerkey,
    p.productid as productkey,
    r.unitprice,
    r.quantity
FROM retail_cleaned r
INNER JOIN dimproduct p ON p.stockcode = r.stockcode
AND p.description = r.description

INNER JOIN dimcustomer c 
ON r.customerid = c.customerid

INNER JOIN dimdate dd 
ON DATE(dd.invoicedate) = DATE(r.invoicedate);
