import psycopg2

def create_databases(user, password, host, port, db_name, dbs):
    conn = psycopg2.connect(
        dbname= db_name,
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.autocommit = True  # Ensure autocommit is enabled for DDL statements
    cursor = conn.cursor()
    
    for db in dbs:
        try:
            cursor.execute(f"CREATE DATABASE {db};")
            print(f"database {db} is created.")
        except Exception as e:
            print(e)

    cursor.close()
    conn.close()

from sqlalchemy import create_engine

# Define your database connection
engine = create_engine('postgresql://username:password@localhost:5432/database_name')

sql_scripts = [
    """
    ALTER TABLE dimcustomer
    ADD CONSTRAINT pk_dimcustomer_customerid PRIMARY KEY ("CustomerID");
    """,
    """
    ALTER TABLE dimdate
    ADD CONSTRAINT pk_dimdate_datekey PRIMARY KEY ("DateKey");
    """,
    """
    ALTER TABLE dimproduct
    ADD CONSTRAINT pk_dimproduct_productkey PRIMARY KEY ("productKey");
    """,
    """
    ALTER TABLE fact
    ADD CONSTRAINT fk_fact_table_customerkey
    FOREIGN KEY ("CustomerKey")
    REFERENCES dimcustomer("CustomerID")
    ON UPDATE CASCADE
    ON DELETE CASCADE;
    """,
    """
    ALTER TABLE fact
    ADD CONSTRAINT fk_fact_table_product
    FOREIGN KEY ("productKey")
    REFERENCES dimproduct("productKey")
    ON UPDATE CASCADE
    ON DELETE CASCADE;
    """,
    """
    ALTER TABLE fact
    ADD CONSTRAINT fk_fact_date
    FOREIGN KEY ("DateKey")
    REFERENCES dimdate("DateKey")
    ON UPDATE CASCADE
    ON DELETE CASCADE;
    """,
    """
    SELECT constraint_name, constraint_type
    FROM information_schema.table_constraints     
    WHERE table_name = 'fact';
    """
]

drop_constraints=[
    """
        ALTER TABLE fact
        DROP CONSTRAINT IF EXISTS fk_fact_table_customerkey
    """,
    """
        ALTER TABLE fact
        DROP CONSTRAINT IF EXISTS fk_fact_table_product
    """,
    """
        ALTER TABLE fact
        DROP CONSTRAINT IF EXISTS fk_fact_date
    """
]
def constraints_metadata(db_user, db_pass, db_host, db_port, db_name, scripts:list= sql_scripts):
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )
    conn.autocommit = True  # Ensure autocommit is enabled for DDL statements
    cursor = conn.cursor()
    for constraint in scripts:
        cursor.execute(constraint)
        
