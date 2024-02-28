import pandas as pd
from sqlalchemy import create_engine
import metadata
def db_connection(user, password, host, port,db):
    engine= create_engine(
         f'postgresql://{user}:{password}@{host}:{port}/{db}'
    )
    engine.connect()
    return engine

def Data_Stage_PG(engine):
    df_iter = pd.read_csv('/opt/airflow/spark/resources/online_retail_1.csv', iterator=True, chunksize=100000)
    df= next(df_iter)
    df.head(n=0).to_sql(name='raw_data', con=engine, if_exists='replace', index=True, index_label='Id')
    try:
        while True:
            s_time= pd.Timestamp.now()
            df.to_sql(name='raw_data', con=engine, if_exists='append', index=True, index_label='Id')
            e_time= pd.Timestamp.now()
            print("- Inserted:",df['InvoiceNo'].count(), f"Records, in {e_time - s_time}s")    
            df= next(df_iter)
    except:
        print("Done...")
        
if __name__== "__main__":

    # try:
    #     # List of database names to create
    #     databases = ['retaildwh']
    #     metadata.create_databases('airflow', 'airflow', '172.26.0.2', 5432, databases)
    #     print("Databases Created Successfully.")
    # except Exception as e:
    #     print("Got Error in Creating the databases.", e)
            
    try:
        db_engine= db_connection(user='airflow', password='airflow', host='postgres', port=5432 ,db='retaildwh')
        print(f"Successfully connected to PG Database.")
    except Exception as e:
        print("Got error in Engine-Connection: ", e)
        
    try:
        Data_Stage_PG(engine=db_engine)
    except Exception as e:
        print("Got error while Writing in the Database: ",e)