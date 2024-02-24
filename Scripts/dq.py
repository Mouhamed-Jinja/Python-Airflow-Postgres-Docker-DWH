import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text

def engine_connection(user, password, host, port,db):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    connection= engine.connect()
    return engine, connection
try:
    engine, connection =engine_connection('postgres', 'postgres','localhost', 5432,'retaildwh')
    print("Connection Done (: ")
except Exception as e:
    print("Got ERROR in Connection for Writing...", e)

new_dimcustomer = pd.read_sql(text('select * from dimcustomer limit 1'), con=connection)

def DQ_Dimcustomer(new_data=new_dimcustomer, engine_conn):
    # New Data is the dim customer that will be append to the dimantion
    dc_info= pd.DataFrame(
        'new_columns': new_data.columns,
        'data_types': new_data.dtypes
    )
   