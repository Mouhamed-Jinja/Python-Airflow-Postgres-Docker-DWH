drop_constraints = [
    """
    ALTER TABLE fact_sales
    DROP CONSTRAINT IF EXISTS fact_sales_customerkey_fkey
    """,
    """
    ALTER TABLE fact_sales
    DROP CONSTRAINT IF EXISTS fact_sales_productkey_fkey
    """,
    """
    ALTER TABLE fact_sales
    DROP CONSTRAINT IF EXISTS fact_sales_datekey_fkey
    """
]

create_constraints = [
    """
    DO
    $$
    BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fact_sales_customerkey_fkey'
        AND conrelid = 'fact_sales'::regclass
    )
    THEN
        ALTER TABLE fact_sales
        ADD CONSTRAINT fact_sales_customerkey_fkey FOREIGN KEY (CustomerKey) REFERENCES dimcustomer(CustomerID) ON UPDATE CASCADE ON DELETE CASCADE;
    END IF;
    END$$;
    """,
    """
    DO
    $$
    BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fact_sales_productkey_fkey'
        AND conrelid = 'fact_sales'::regclass
    )
    THEN
        ALTER TABLE fact_sales
        ADD CONSTRAINT fact_sales_productkey_fkey FOREIGN KEY (ProductKey) REFERENCES dimproduct(ProductID) ON UPDATE CASCADE ON DELETE CASCADE;
    END IF;
    END$$;
    """,
    """
    DO
    $$
    BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fact_sales_datekey_fkey'
        AND conrelid = 'fact_sales'::regclass
    )
    THEN
        ALTER TABLE fact_sales
        ADD CONSTRAINT fact_sales_datekey_fkey FOREIGN KEY (DateKey) REFERENCES dimdate(DateKey) ON UPDATE CASCADE ON DELETE CASCADE;
    END IF;
    END$$;
    """
]
