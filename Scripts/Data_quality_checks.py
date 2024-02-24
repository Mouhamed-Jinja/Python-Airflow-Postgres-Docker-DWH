import pandas as pd
from sqlalchemy import text  # Importing text function from sqlalchemy

def DIM_Product_DQ(new_data, dim, engine):
    new_info = pd.DataFrame({
        'new_columns': new_data.columns,
        'new_datatypes': new_data.dtypes
    })
    
    old_data = pd.read_sql(text(f'select * from {dim} limit 1;'), con=engine)
    old_info = pd.DataFrame({
        'old_columns': old_data.columns,
        'old_datatypes': old_data.dtypes
    })

    merged_info = pd.merge(new_info, old_info, left_index=True, right_index=True)

    if not ((merged_info['new_columns'] == merged_info['old_columns']).all() and (merged_info['new_datatypes'] == merged_info['old_datatypes']).all()):
        raise ValueError(f"Data Quality Checks for {dim}: Mismatch in Column names or Datatypes. Pipeline halted.")
    else:
        print(f"Data Quality Checks in {dim}: Got Match in Columns names and Datatypes")
