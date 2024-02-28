import pandas as pd
from sqlalchemy import create_engine, text
import Data_quality_checks
import metadata
def database_engine(user, password, host, port, db):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    connection = engine.connect()
    return engine, connection


def Load_DimProduct(engine_conn):
    try:
        with open('/opt/airflow/spark/app/sql_scd/dimproduct.sql') as file:
            sql_query = file.read()
        results = engine_conn.execute(text(sql_query)).fetchall()
    except Exception as e:
        print('Got ERROR in Product Dimantion, While executing the SQL Query:',e)
        
    dimproduct_latest = pd.DataFrame(results)
    dimproduct_latest['productid']= range(1,len(dimproduct_latest)+1, 1)
    dimproduct_latest['name']= 'Product-'+dimproduct_latest['stockcode']
    dimproduct_latest['processed_date']= pd.to_datetime(dimproduct_latest['processed_date'])
    dimproduct_latest= dimproduct_latest.reindex(columns=['productid', 'name', 'stockcode', 'description', 'processed_date'])
    
    #check data quality before writing it.
    try:
        Data_quality_checks.dim_data_quality_check(new_data=dimproduct_latest, dim='dimproduct',engine=connection)
    except Exception as e:
        print(e)
    dimproduct_latest= dimproduct_latest.set_index('productid')
    try:
        dimproduct_latest.to_sql(name='dimproduct', con=engine_conn, if_exists='replace', index=True, index_label='productid')
        engine_conn.commit()
    except Exception as e:
        print("Got ERROR in Product Dimantion: While writing the DF:", e)
 

def Load_DimCustomer(engine_conn):
    try:
        with open('/opt/airflow/spark/app/sql_scd/dimcustomer.sql') as file:
            query = file.read()
            
        result = engine_conn.execute(text(query)).fetchall()
    except Exception as e:
        print('Error executing SQL query for Customer Dimension:', e)
        return
    
    if not result:
        print('No data retrieved for Customer Dimension.')
        return
    
    latest_customers = pd.DataFrame(result)
    latest_customers['name'] = 'Customer' + latest_customers['customerid'].astype(str)
    latest_customers = latest_customers.reindex(columns=['customerid', 'country', 'name', 'processed_date'])
    
    # check data quality before writing it.
    Data_quality_checks.dim_data_quality_check(new_data=latest_customers, dim='dimcustomer',engine=connection)

    latest_customers = latest_customers.set_index('customerid')
    try:
        latest_customers.to_sql(name='dimcustomer', con=engine_conn, if_exists='replace', index=True, index_label='customerid')
        engine_conn.commit()
    except Exception as e:
        print("Error writing DataFrame to dimcustomer table:", e)


def Load_DimDate(engine_conn):
    try:
        old_max = """
        select max(invoicedate) as max_date from dimdate
        """
        new_max = """
        select max(invoicedate) as max_date from retail_cleaned
        """
        old_max_date_df = pd.read_sql(text(old_max), con=engine_conn)
        new_max_date_df = pd.read_sql(text(new_max), con=engine_conn)
    except Exception as e:
        print("Got ERROR in Date Dimantion, While executing the SQL Query:",e)
        
    old_max_date = old_max_date_df['max_date'].iloc[0]
    new_max_date = new_max_date_df['max_date'].iloc[0]
    
    if new_max_date > old_max_date:
        
        start_range = old_max_date + pd.Timedelta(days=1)
        end_range = new_max_date
        print(f" Start:{start_range}, End:{end_range}")
        Dimdate = pd.DataFrame({'InvoiceDate': pd.date_range(start=start_range, end=end_range, freq='D')})
        Dimdate['DateKey'] = Dimdate['InvoiceDate'].dt.strftime('%Y-%m-%d')
        Dimdate['Year'] = Dimdate['InvoiceDate'].dt.strftime('%Y')
        Dimdate['MonthNo'] = Dimdate['InvoiceDate'].dt.strftime('%m')
        Dimdate['MonthName'] = Dimdate['InvoiceDate'].dt.strftime('%B')
        Dimdate['Day'] = Dimdate['InvoiceDate'].dt.strftime('%d')
        Dimdate['quarter'] = Dimdate['InvoiceDate'].dt.quarter
        Dimdate.columns = Dimdate.columns.str.lower()
        
        # check data quality before writing it.
        Data_quality_checks.dim_data_quality_check(new_data=Dimdate, dim='dimdate',engine=connection)

        Dimdate = Dimdate.reset_index(drop=True).set_index('datekey')
        try:
            Dimdate.to_sql(name='dimdate', con=engine_conn, if_exists='append', index=True, index_label='datekey')
            engine_conn.commit()
        except Exception as e:
            print("Got ERROR in Date Dimantion, While Writing the DF",e)
     
    else:
        print("The New Dates already exists in the dimantion")

def Fact_Sales(engine_conn):
    try:
        with open('/opt/airflow/spark/app/sql_scd/fact_sales.sql') as file:
            query= file.read()
        result= engine_conn.execute(text(query))
        factDF= pd.DataFrame(result)
        max_saleskey_df = pd.read_sql(text('select max(saleskey) as max_date from fact_sales'), con= engine_conn)
    except Exception as e:
        print("Got ERROR in Fact Sales table: while Executing the SQL Script")
        
    max_saleskey_id = max_saleskey_df['max_date'].iloc[0] +1
    factDF['saleskey']= range(max_saleskey_id, len(factDF)+max_saleskey_id, 1)
    
    
    new_order = ['saleskey', 'invoiceno', 'datekey', 'invoicedate', 'customerkey', 'productkey', 'unitprice', 'quantity']
    factDF = factDF.reindex(columns=new_order)
    print(factDF.head())
    
    # check data quality before writing it.
    Data_quality_checks.dim_data_quality_check(new_data=factDF, dim='fact_sales',engine=connection)
    
    factDF= factDF.set_index('saleskey')
    try:
        factDF.to_sql(name='fact_sales', con= engine_conn, if_exists='append', index=True, index_label='saleskey') 
        engine_conn.commit()
    except Exception as e:
        print("Got ERROR in Fact Sales table:", e)

if __name__== "__main__":
    try:
        engine, connection = database_engine('airflow', 'airflow', 'postgres', 5432, 'retaildwh')
    except Exception as e:
        print("Got ERROR in connection to DB:", e)
    
    try:
        for constraints in metadata.drop_constraints:
            connection.execute(text(constraints))
            connection.commit()
        print("Constraints dropped.")
    except Exception as e:
        print("Got Error while dropping the constraints")
        
    try:
        Load_DimProduct(engine_conn= connection)
        print("DIM Product loaded successfully.")
    except Exception as e:
        print("Got ERROR in loading DIM Product",e)
    
    try:
        Load_DimCustomer(engine_conn= connection)
        print("DIM Customer loaded successfully")
    except Exception as e:
        print("Got ERROR in loading DIM Customer",e)
        
    try:
        Load_DimDate(engine_conn= connection)
        print("DIM Date loaded successfully.")
    except Exception as e:
        print("Got ERROR in loading DIM Date",e)
        
    try:
        Fact_Sales(engine_conn= connection)
        print("Fact Sales Table loaded successfully.")
    except Exception as e:
        print("Got ERROR in loading Fact Sales table",e)
    