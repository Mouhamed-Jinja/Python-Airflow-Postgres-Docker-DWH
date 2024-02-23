import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def read_connection(user, password, host, port,db):
    engine= psycopg2.connect(
         user= user,
         password= password,
         host= host,
         port=port,
         database= db
    )
    return engine

def execute_query(query, engine, index_col):
    df = pd.read_sql(query, engine, index_col=index_col)
    return df

def data_cleanings(raw_data):
    # Exclude nulls
    df= raw_data[~raw_data['Description'].isna()]
    df= df[~df['CustomerID'].isna()]
    
    # Drop duplicates
    df= df.drop_duplicates()
    
    # No quantity in negative, so i will convert all negative values
    df.loc[df['Quantity'] < 0, 'Quantity'] = df.loc[df['Quantity'] < 0, 'Quantity'] * -1
    
    # Handling Outliers in Quantity
    first_quartile =df['Quantity'].quantile(0.25)
    third_quartile =df['Quantity'].quantile(0.75)
    IQR= third_quartile - first_quartile
    max_outlier= IQR*10+third_quartile  #max = IQR*10+Q3 threshold here is 10
    df.loc[df['Quantity'] > max_outlier, 'Quantity'] = max_outlier
    
    # Handling Outliers in Price
    Q1= df['UnitPrice'].quantile(0.25)
    Q3= df['UnitPrice'].quantile(0.75)
    IQR= Q3-Q1
    max_outlier= IQR*1.5+Q3  #not used
    df.loc[df['UnitPrice'] >750, 'UnitPrice']= 750
    
    
    

    # Update datatypes
    df['StockCode']= df['StockCode'].astype(str)
    df['InvoiceDate']= pd.to_datetime( df['InvoiceDate'])
    df['CustomerID']= df['CustomerID'].astype(int)
    df['InvoiceNo']= df['InvoiceNo'].astype(str)

    df.columns = df.columns.str.lower()
    
    
    return df

def write_connection(user, password, host, port,db):
    engine= create_engine(
            f'postgresql://{user}:{password}@{host}:{port}/{db}'
        )
    engine.connect()
    return engine   


if __name__ == "__main__":

    # Read raw data
    try:
        db_engine= read_connection(user='postgres', password='postgres', host='localhost', port=5432 ,db='retaildwh')
        print("Connection done successfully...")
    except Exception as e:
        print("Got ERROR in Connection to DB for Reading", e)
    
    try:    
        query = '''
            SELECT * 
            FROM raw_data
        '''
        raw_data = pd.read_sql(query, db_engine, index_col='Id')
        print("The Raw Data Loaded Successfully")
        
    except Exception as e:
        print("Got ERROR in your query or index", e)
    
        
    # Start Cleaning 
    try:
        cleanedDF= data_cleanings(raw_data)
        
        print(cleanedDF.head(5))
        print(cleanedDF.dtypes)
        print("Cleaning Done...")
    except Exception as e:
        print("Got ERROR in Data Cleaning part...", e)
    
      
    # Stage the Denormalized Cleaned Data
    try:
        engine= write_connection(user='postgres', password='postgres', host='localhost', port=5432 ,db='retaildwh')
        print("The connection for Writing Done Successfully...")
    except Exception as e:
        print("Got ERROR in Connection For Writing...",e)
        
    try:
        print("Start Creating the Table: retail_cleaned")
        cleanedDF.head(n=0).to_sql(name='retail_cleaned', con=engine, if_exists='replace', index=True, index_label='Id')
        print("Creation Done, Start Loading data in it...")
        cleanedDF.to_sql(name='retail_cleaned', con=engine, if_exists='append', index=True, index_label='Id')
        
        print('Finished (:')
    except Exception as e:
        print("Got ERROR in Staging the Cleaned Data.", e)
    
        
    
