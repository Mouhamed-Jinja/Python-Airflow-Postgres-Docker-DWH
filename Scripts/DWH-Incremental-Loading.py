def update_dimcustomer(cleaned_df, db_conn):
    # Load current dimension data
    current_dim_data = pd.read_sql('SELECT * FROM dimcustomer', db_conn, index_col='CustomerID')

    # Identify changes
    new_records = cleaned_df[~cleaned_df.index.isin(current_dim_data.index)]
    updated_records = cleaned_df[cleaned_df.index.isin(current_dim_data.index) & 
                                 ~cleaned_df.equals(current_dim_data)]

    # Update existing records
    if not updated_records.empty:
        updated_records.to_sql(name='dimcustomer', con=db_conn, if_exists='replace', index=True,
                               index_label='CustomerID', method='multi', chunksize=10000)

    # Insert new records
    if not new_records.empty:
        new_records.to_sql(name='dimcustomer', con=db_conn, if_exists='append', index=True,
                           index_label='CustomerID', method='multi', chunksize=10000)

    return new_records, updated_records

# Call the function
new_records, updated_records = update_dimcustomer(df, db_engine)
