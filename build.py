import psycopg2
sql_statements = [
    """
    -- Create dimcustomer table
    CREATE TABLE dimcustomer (
        CustomerID BIGINT PRIMARY KEY,
        Country TEXT,
        name TEXT,
        processed_date DATE
    )
    """,
    """
    -- Create dimdate table
    CREATE TABLE dimdate (
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
    CREATE TABLE dimproduct(
        ProductID BIGINT PRIMARY KEY,
        name TEXT,
        StockCode TEXT,
        Description TEXT,
        processed_date DATE
    )
    """,
    """
    -- Create fact_table table
    CREATE TABLE fact_sales(
        saleskey BIGINT PRIMARY KEY,
        InvoiceNo TEXT,
        DateKey TIMESTAMP,
        CustomerKey BIGINT,
        ProductKey BIGINT,
        UnitPrice DOUBLE PRECISION,
        Quantity BIGINT,
        FOREIGN KEY (DateKey) REFERENCES dimdate (DateKey) ON DELETE CASCADE ON UPDATE CASCADE,
        FOREIGN KEY (CustomerKey) REFERENCES dimcustomer (CustomerID) ON DELETE CASCADE ON UPDATE CASCADE,
        FOREIGN KEY (ProductKey) REFERENCES dimproduct (ProductID) ON DELETE CASCADE ON UPDATE CASCADE
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
constraints_metadata('postgres', 'postgres', 'localhost',5432,'retaildwh')
