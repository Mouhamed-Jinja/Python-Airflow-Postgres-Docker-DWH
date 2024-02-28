import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date

def db_engine(user, password, host, port, db):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    connection = engine.connect()
    return engine, connection

def Dimcustomer(cleaned_df, db_conn):
    DimCust = cleaned_df[['customerid', 'country']].copy()
    DimCust.columns= DimCust.columns.str.lower()
    DimCust['name'] = 'Customer' + DimCust['customerid'].astype(str)
    DimCust= DimCust.reset_index(drop=True)
    DimCust= DimCust.drop_duplicates('customerid').set_index('customerid')
    DimCust['processed_date']= date.today()

    #DimCust.head(n=0).to_sql(name='dimcustomer', con=db_conn, if_exists='replace', index=True, index_label='CustomerID')
    DimCust.to_sql(name='dimcustomer', con=db_conn, if_exists='append', index=True, index_label='customerid')
    db_conn.commit()

def DimProduct(cleanedDF, db_conn):
    dimpro = cleanedDF[['stockcode', 'description']]\
        .drop_duplicates(subset=['stockcode', 'description'])\
        .reset_index(drop=True)

    # Generate 'productid' values as a range starting from start_productid
    start_productid=1
    dimpro['productid'] = range(start_productid, start_productid + len(dimpro))

    dimpro['name'] = 'Product-' + dimpro['stockcode']
    dimpro['processed_date'] = date.today()
    dimpro = dimpro.reindex(columns=['productid', 'name', 'stockcode', 'description', 'processed_date'])

    # Ensure 'productid' is set as the index
    dimpro.set_index('stockcode', inplace=True)

    # Add the DataFrame to the 'dimproduct' table
    dimpro.to_sql(name='dimproduct', if_exists='append', con=db_conn, index=True, index_label='stockcode')
    db_conn.commit()
    return dimpro




def DimDate(cleanedDF, db_conn):
    cleanedDF['invoicedate']= pd.to_datetime(cleanedDF['invoicedate'])
    maxdate= cleanedDF['invoicedate'].max()
    mindate= cleanedDF['invoicedate'].min()
    Dimdate = pd.DataFrame({'InvoiceDate': pd.date_range(start=mindate, end=maxdate, freq='D')})
    Dimdate['DateKey'] = Dimdate['InvoiceDate'].dt.strftime('%Y-%m-%d')
    Dimdate['Year']= Dimdate['InvoiceDate'].dt.strftime('%Y')
    Dimdate['MonthNo']= Dimdate['InvoiceDate'].dt.strftime('%m')
    Dimdate['MonthName']= Dimdate['InvoiceDate'].dt.strftime('%B')
    Dimdate['Day']= Dimdate['InvoiceDate'].dt.strftime('%d')
    Dimdate['quarter']= Dimdate['InvoiceDate'].dt.quarter
    Dimdate.columns= Dimdate.columns.str.lower()
    Dimdate= Dimdate.reset_index(drop=True).set_index('datekey')
    print(Dimdate.head())
    #Dimdate.head(n=0).to_sql(name= 'dimdate', con= db_conn, if_exists= 'replace',index=True, index_label='DateKey')
    Dimdate.to_sql(name= 'dimdate', con= db_conn, if_exists= 'replace',index=True, index_label='datekey')
    db_conn.commit()

def FactTable(cleaned_data, DimProduct, db_conn):
    cleaned_data.columns= cleaned_data.columns.str.lower()
    try:
        fact =cleaned_data.merge(DimProduct, on=['stockcode','description']).copy()
    except Exception as e:
        print("print error in ",e)
    fact['saleskey']= range(1, len(fact)+1,1)
    fact['DateKey'] = fact['invoicedate'].dt.strftime('%Y-%m-%d')
    fact.columns= fact.columns.str.lower()
    fact= fact[['saleskey','invoiceno', 'datekey', 'invoicedate', 'customerid', 'productid','unitprice','quantity']]\
        .rename(columns={
            'customerid':'customerkey',
            'productid': 'productkey'})\
        .set_index('saleskey')
    print(fact.head())

    #fact.head(n=0).to_sql(name='fact', con= db_conn,if_exists='replace', index=True, index_label='InvoiceNo')
    fact.to_sql(name='fact_sales', con= db_conn, if_exists='append', index=True, index_label='saleskey')
    db_conn.commit()
if __name__ == "__main__":

    # Read Cleaned Data to Start Modeling the Star Schema...
    try:
        engine, connection = db_engine('airflow', 'airflow', 'postgres', 5432, 'retaildwh')
        print("Connection done successfully for reading...")
    except Exception as e:
        print("Got ERROR in Connection to DB for Reading", e)
    
    try:    
        query = '''
            SELECT * 
            FROM retail_cleaned
        '''
        cleaned_data = pd.read_sql(text(query), connection, index_col='Id')
        print("The Cleaned Data Loaded Successfully")
    except Exception as e:
        print("Got ERROR in your query or index", e)
        
 
    # #Drop any constraints befor updates:
    # try:
    #     metadata.constraints_metadata('postgres', 'postgres', 'localhost', 5432, "gold", scripts=metadata.drop_constraints)
    #     print("Constrants if exists have been droped")
    # except Exception as e:
    #     print(e)
        
    # Calling the Dimantions and write it.
    print("---> Start Writing the Dimantions...")
    
    #DimCustomer:-
    try:
        Dimcustomer(cleaned_data,connection)
        print("DimCustomer has been written.")
    except Exception as e:
        print("Got ERROR in Customer Dimantion", e)
        
    #DimProduct:-
    try:
        dimproduct= DimProduct(cleaned_data,connection)
        print("DimProduct has been written.")
    except Exception as e:
        print("Got ERROR in Product Dimantion", e)
        
        
    #DimDate:-
    try:
        DimDate(cleaned_data,connection)
        print("DimDate has been written.")
    except Exception as e:
        print("Got ERROR in Date Dimantion", e)
        
    #Fact Table:-
    try:
        FactTable(cleaned_data, dimproduct, connection)
        print("FactTable has been written.")
    except Exception as e:
        print("Got ERROR in FactTable", e)
        
    # # try:
    # #     metadata.constraints_metadata('postgres', 'postgres', 'localhost', 5432, "gold")
    # #     print("Metadata Maintained")
    # # except Exception as e:
    # #     print(e)