import pandas as pd
from sqlalchemy import create_engine, text

def db_engine(user, password, host, port, db):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    connection = engine.connect()
    return engine, connection

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
    df['Quantity']= df['Quantity'].astype(int)

    df.columns = df.columns.str.lower()
    
    
    return df

if __name__ == "__main__":

    # Read raw data
    try:
        engine, connection = db_engine('postgres', 'postgres', 'localhost', 5432, 'retaildwh')
    except Exception as e:
        print("Got ERROR in connection to DB:", e)
    
    try:    
        query = '''
            SELECT * 
            FROM raw_data
        '''
        raw_data = pd.read_sql(text(query), connection, index_col='Id')
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
        print("Start Creating the Table: retail_cleaned")
        print("Creation Done, Start Loading data in it...")
        cleanedDF.to_sql(name='retail_cleaned', con=connection, if_exists='replace', index=True, index_label='Id')
        connection.commit()
        print(cleanedDF)
        print('Finished (:')
    except Exception as e:
        print("Got ERROR in Staging the Cleaned Data.", e)
    
        
    
