-- Create dimcustomer table
CREATE TABLE dimcustomer (
    CustomerID BIGINT PRIMARY KEY,
    Country TEXT,
    name TEXT
);

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
);

-- Create dimproduct table
CREATE TABLE dimproduct (
    StockCode TEXT,
    Description TEXT,
    ProductID BIGINT PRIMARY KEY,
    name TEXT
);

-- Create fact_table table
CREATE TABLE fact_table (
    InvoiceNo TEXT PRIMARY KEY,
    DateKey TIMESTAMP,
    CustomerKey BIGINT,
    ProductKey BIGINT,
    UnitPrice DOUBLE PRECISION,
    Quantity BIGINT,
    FOREIGN KEY (DateKey) REFERENCES dimdate (DateKey) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (CustomerKey) REFERENCES dimcustomer (CustomerID) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (ProductKey) REFERENCES dimproduct (ProductID) ON DELETE CASCADE ON UPDATE CASCADE
);
