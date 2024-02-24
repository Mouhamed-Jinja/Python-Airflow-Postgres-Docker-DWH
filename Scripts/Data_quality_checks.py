import pandas as pd
from sqlalchemy import text  # Importing text function from sqlalchemy

def dim_data_quality_check(new_data, dim, engine):
    new_info = pd.DataFrame({
        'new_columns': new_data.columns,
        'new_datatypes': new_data.dtypes
    })
    print(f"-----------{dim}------------")

    new_info= new_info.sort_values(['new_columns', 'new_datatypes'])
    print("---NEW---","\t",new_info)
    print(dim)
    old_data = pd.read_sql(text(f'SELECT * FROM {dim} LIMIT 1'), con=engine)
    
    old_info = pd.DataFrame({
        'old_columns': old_data.columns,
        'old_datatypes': old_data.dtypes
    })
    old_info= old_info.sort_values(['old_columns', 'old_datatypes'])
    print(old_info)
    merged_info = pd.merge(new_info, old_info, left_index=True, right_index=True)

    if not ((merged_info['new_columns'] == merged_info['old_columns']).all() and (merged_info['new_datatypes'] == merged_info['old_datatypes']).all()):
        raise ValueError(f"Data Quality Checks for {dim}: Mismatch in Column names or Datatypes. Pipeline halted.")
    else:
        print(f"Data Quality Checks in {dim}: Got Match in Columns names and Datatypes")
