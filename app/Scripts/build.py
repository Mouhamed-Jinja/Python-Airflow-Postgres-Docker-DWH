import psycopg2
sql_statements = [
    
    """
    -- Create dimcustomer table
    CREATE TABLE IF NOT EXISTS dimcustomer (
        CustomerID BIGINT PRIMARY KEY,
        Country TEXT,
        name TEXT,
        processed_date TIMESTAMP
    )
    """,
    """
    -- Create dimdate table
    CREATE TABLE IF NOT EXISTS dimdate (
        DateKey TIMESTAMP PRIMARY KEY,
        InvoiceDate TIMESTAMP,
        Date TEXT,
        Year TEXT,
        MonthNo TEXT,
        MonthName TEXT,
        Day TEXT,
        quarter BIGINT
    )
    """,
    """
    -- Create dimproduct table
    CREATE TABLE IF NOT EXISTS dimproduct(
        ProductID BIGINT PRIMARY KEY,
        name TEXT,
        StockCode TEXT,
        Description TEXT,
        processed_date TIMESTAMP
    )
    """,
    """
    -- Create fact_table table
    CREATE TABLE IF NOT EXISTS fact_sales(
        saleskey BIGINT PRIMARY KEY,
        InvoiceNo TEXT,
        datekey TIMESTAMP,
        invoicedate TIMESTAMP,
        CustomerKey BIGINT,
        ProductKey BIGINT,
        UnitPrice DOUBLE PRECISION,
        Quantity BIGINT
        --FOREIGN KEY (DateKey) REFERENCES dimdate (DateKey) ON DELETE CASCADE ON UPDATE CASCADE,
        --FOREIGN KEY (CustomerKey) REFERENCES dimcustomer (CustomerID) ON DELETE CASCADE ON UPDATE CASCADE,
        --FOREIGN KEY (ProductKey) REFERENCES dimproduct (ProductID) ON DELETE CASCADE ON UPDATE CASCADE
    )
    """
]

# Execute SQL statements
def constraints_metadata(db_user, db_pass, db_host, db_port, db_name, scripts:list= sql_statements):
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
    conn.autocommit = True  # Ensure autocommit is enabled for DDL statements
    cursor = conn.cursor()
    for constraint in sql_statements:
        cursor.execute(constraint)
        print('created')
    
constraints_metadata('airflow', 'airflow', 'postgres', 5432, 'retaildwh')
