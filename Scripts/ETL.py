import pandas as pd
from sqlalchemy import create_engine, text

def db_engine(user, password, host, port, db):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    connection = engine.connect()
    return engine, connection

engine, connection = db_engine('postgres', 'postgres', 'localhost', 5432, 'retaildwh')


def Load_DimProduct(engine= connection):
    try:
        with open('../ETL-SCD-Queries/dimproduct.sql', 'r') as file:
            sql_query = file.read()
        results = engine.execute(text(sql_query)).fetchall()
    except Exception as e:
        print('Got ERROR in Product Dimantion, While executing the SQL Query:',e)
        
    dimproduct_latest = pd.DataFrame(results)
    dimproduct_latest['productid']= range(1,len(dimproduct_latest)+1, 1)
    dimproduct_latest['name']= 'Product-'+dimproduct_latest['stockcode']
    dimproduct_latest['processed_date']= pd.to_datetime(dimproduct_latest['processed_date'])
    dimproduct_latest= dimproduct_latest.reindex(columns=['productid', 'name', 'stockcode', 'description', 'processed_date'])
    dimproduct_latest= dimproduct_latest.set_index('productid')
    
    try:
        dimproduct_latest.to_sql(name='dimproduct', con=engine, if_exists='replace', index=True, index_label='productid')
    except Exception as e:
        print("Got ERROR in Product Dimantion: While writing the DF:", e)
 
def load_dimcustomer(conn= connection):
    try:
        with open('../ETL-SCD-Queries/dimcustomer.sql') as file:
            query= file.read()
        result= conn.execute(text(query)).fetchall()
    except Exception as e:
        print('Got ERROR in Customer Dimantion, While executing the SQL Query:',e)
        
    latest_customers= pd.DataFrame(result)
    latest_customers['name']= 'Customer'+latest_customers['customerid'].astype(str)
    latest_customers= latest_customers.reindex(columns=['customerid', 'country', 'name', 'processed_date'])
    latest_customers= latest_customers.set_index('customerid')
    
    try:
        latest_customers.to_sql(name='dimcustomer', con=conn, if_exists='replace', index=True, index_label='customerid')
    except Exception as e:
        print("Got ERROR in Customer Dimantion: While writing the DF:", e)

def load_dimdate(db_connection=connection):
    try:
        old_max = """
        select max(invoicedate) as max_date from dimdate
        """
        new_max = """
        select max(invoicedate) as max_date from retail_cleaned
        """
        old_max_date_df = pd.read_sql(text(old_max), con=db_connection)
        new_max_date_df = pd.read_sql(text(new_max), con=db_connection)
    except Exception as e:
        print("Got ERROR in Date Dimantion, While executing the SQL Query:",e)
        
    old_max_date = old_max_date_df['max_date'].iloc[0]
    new_max_date = new_max_date_df['max_date'].iloc[0]
    
    if new_max_date > old_max_date:
        
        start_range = old_max_date + pd.Timedelta(days=1)
        end_range = new_max_date
        print(f" Start:{start_range}, End:{end_range}")
        Dimdate = pd.DataFrame({'InvoiceDate': pd.date_range(start=start_range, end=end_range, freq='D')})
        Dimdate['DateKey'] = Dimdate['InvoiceDate']
        Dimdate['Date'] = Dimdate['InvoiceDate'].dt.strftime('%Y-%m-%d')
        Dimdate['Year'] = Dimdate['InvoiceDate'].dt.strftime('%Y')
        Dimdate['MonthNo'] = Dimdate['InvoiceDate'].dt.strftime('%m')
        Dimdate['MonthName'] = Dimdate['InvoiceDate'].dt.strftime('%B')
        Dimdate['Day'] = Dimdate['InvoiceDate'].dt.strftime('%d')
        Dimdate['quarter'] = Dimdate['InvoiceDate'].dt.quarter
        Dimdate.columns = Dimdate.columns.str.lower()
        Dimdate = Dimdate.reset_index(drop=True).set_index('datekey')
        try:
            Dimdate.to_sql(name='dimdate', con=db_connection, if_exists='append', index=True, index_label='datekey')
        except Exception as e:
            print("Got ERROR in Date Dimantion, While Writing the DF",e)
     
    else:
        print("The New Dates already exists in the dimantion")

def Fact_Sales(conn= connection):
    
    try:
        with open('../ETL-SCD-Queries/fact_sales.sql') as file:
            query= file.read()
        result= conn.execute(text(query))
        factDF= pd.DataFrame(result)
        max_saleskey_df = pd.read_sql(text('select max(saleskey) as max_date from fact_sales'), con=conn)
    except Exception as e:
        print("Got ERROR in Fact Sales table: while Executing the SQL Script")
        
    max_saleskey_id = max_saleskey_df['max_date'].iloc[0] +1
    factDF['saleskey']= range(max_saleskey_id, len(factDF)+max_saleskey_id, 1)
    factDF= factDF.set_index('saleskey')
    try:
        factDF.to_sql(name='fact_sales', con= conn, if_exists='append', index=True, index_label='saleskey')
    except Exception as e:
        print("Got ERROR in Fact Sales table:", e)


