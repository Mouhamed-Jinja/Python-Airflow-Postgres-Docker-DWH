import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import metadata
from datetime import date


def read_connection(db_password, db_user, db_host, db_port, db_name):
    engine= psycopg2.connect(
        user= db_user,
        password= db_password,
        host= db_host,
        port= db_port,
        database= db_name
        
    )
    return engine

def write_connection(user, password, host, port,db):
    engine= create_engine(
         f'postgresql://{user}:{password}@{host}:{port}/{db}'
    )
    engine.connect()
    return engine

def Dimcustomer(cleaned_df, db_conn):
    DimCust = cleaned_df[['CustomerID', 'Country']].copy()
    DimCust.columns= DimCust.columns.str.lower()
    DimCust['name'] = 'Customer' + DimCust['customerid'].astype(str)
    DimCust= DimCust.reset_index(drop=True)
    DimCust= DimCust.drop_duplicates('customerid').set_index('customerid')
    #DimCust.head(n=0).to_sql(name='dimcustomer', con=db_conn, if_exists='replace', index=True, index_label='CustomerID')
    DimCust.to_sql(name='dimcustomer', con=db_conn, if_exists='append', index=True, index_label='customerid')


def DimProduct(cleanedDF, db_conn):
    dimpro= cleanedDF[['stockcode', 'description']]\
        .drop_duplicates(subset=['stockcode', 'description'])\
        .reset_index(drop=True)
        
    dimpro['productid']= range(1000, dimpro['stockcode'].count()+1000, 1)
    dimpro['name']= 'Product-'+dimpro['stockcode']
    dimpro= dimpro.set_index('stockcode')
    dimpro['processed_date']= date.today()
    print(dimpro.head())
    dimpro= dimpro.reindex(columns=['productid','name' , 'stockcode' ,'description','processed_date'])
    #dimpro.head(n=0).to_sql(name= 'dimproduct', if_exists= 'replace',con= db_conn, index=True, index_label='StockCode')
    dimpro.to_sql(name= 'dimproduct', if_exists= 'append',con= db_conn, index=True, index_label='stockcode')
    return dimpro



def DimDate(cleanedDF, db_conn):
    cleanedDF['InvoiceDate']= pd.to_datetime(cleanedDF['InvoiceDate'])
    maxdate= cleanedDF['InvoiceDate'].max()
    mindate= cleanedDF['InvoiceDate'].min()
    Dimdate = pd.DataFrame({'InvoiceDate': pd.date_range(start=mindate, end=maxdate, freq='D')})
    Dimdate['DateKey'] = Dimdate['InvoiceDate']
    Dimdate['Date'] = Dimdate['InvoiceDate'].dt.strftime('%Y-%m-%d')
    Dimdate['Year']= Dimdate['InvoiceDate'].dt.strftime('%Y')
    Dimdate['MonthNo']= Dimdate['InvoiceDate'].dt.strftime('%m')
    Dimdate['MonthName']= Dimdate['InvoiceDate'].dt.strftime('%B')
    Dimdate['Day']= Dimdate['InvoiceDate'].dt.strftime('%d')
    Dimdate['quarter']= Dimdate['InvoiceDate'].dt.quarter
    Dimdate.columns= Dimdate.columns.str.lower()
    Dimdate= Dimdate.reset_index(drop=True).set_index('datekey')
    
    #Dimdate.head(n=0).to_sql(name= 'dimdate', con= db_conn, if_exists= 'replace',index=True, index_label='DateKey')
    Dimdate.to_sql(name= 'dimdate', con= db_conn, if_exists= 'append',index=True, index_label='datekey')


def FactTable(cleaned_data, DimProduct, db_conn):
    cleaned_data.columns= cleaned_data.columns.str.lower()
    fact =cleaned_data.merge(DimProduct, on=['stockcode','description']).copy()
    fact.columns= fact.columns.str.lower()
    fact= fact[['saleskey','invoiceno','invoicedate','customerid', 'productid','unitprice','quantity']]\
        .rename(columns={
            'invoicedate':'datekey',
            'customerid':'customerkey',
            'productid': 'productkey'})\
        .set_index('saleskey')
    print(fact.head())

    #fact.head(n=0).to_sql(name='fact', con= db_conn,if_exists='replace', index=True, index_label='InvoiceNo')
    fact.to_sql(name='fact_sales', con= db_conn, if_exists='append', index=True, index_label='saleskey')

if __name__ == "__main__":

    # Read Cleaned Data to Start Modeling the Star Schema...
    try:
        db_engine= read_connection(db_password='postgres',db_user='postgres', db_host='localhost',db_port=5432, db_name='retaildwh')
        print("Connection done successfully for reading...")
    except Exception as e:
        print("Got ERROR in Connection to DB for Reading", e)
    
    try:    
        query = '''
            SELECT * 
            FROM retail_cleaned
        '''
        cleaned_data = pd.read_sql(query, db_engine, index_col='Id')
        print("The Cleaned Data Loaded Successfully")
    except Exception as e:
        print("Got ERROR in your query or index", e)
        
    try:
        print("Test Connection for writing the Dimantions.")
        db_connection =write_connection('postgres', 'postgres','localhost', 5432,'retaildwh')
        print("Connection Done (: ")
    except Exception as e:
        print("Got ERROR in Connection for Writing...", e)
        
    # #Drop any constraints befor updates:
    # try:
    #     metadata.constraints_metadata('postgres', 'postgres', 'localhost', 5432, "gold", scripts=metadata.drop_constraints)
    #     print("Constrants if exists have been droped")
    # except Exception as e:
    #     print(e)
        
    # Calling the Dimantions and write it.
    print("---> Start Writing the Dimantions...")
    
    # #DimCustomer:-
    # try:
    #     Dimcustomer(cleaned_data,db_connection)
    #     print("DimCustomer has been written.")
    # except Exception as e:
    #     print("Got ERROR in Customer Dimantion", e)
        
    #DimProduct:-
    try:
        dimproduct= DimProduct(cleaned_data,db_connection)
        print("DimProduct has been written.")
    except Exception as e:
        print("Got ERROR in Product Dimantion", e)
        
        
    # #DimDate:-
    # try:
    #     DimDate(cleaned_data,db_connection)
    #     print("DimDate has been written.")
    # except Exception as e:
    #     print("Got ERROR in Date Dimantion", e)
        
    # #Fact Table:-
    # try:
    #     FactTable(cleaned_data, dimproduct, db_connection)
    #     print("FactTable has been written.")
    # except Exception as e:
    #     print("Got ERROR in FactTable", e)
        
    # # try:
    # #     metadata.constraints_metadata('postgres', 'postgres', 'localhost', 5432, "gold")
    # #     print("Metadata Maintained")
    # # except Exception as e:
    # #     print(e)