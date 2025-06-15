"""

Human written verifier function for the ETL agent. This function is designed to verify the correctness 
of the full ETL process by checking the state of the lakehouse after the ETL agent has run. 

Please check out the companion blog post for more details, background and potential alternatives to this
setup!

"""

def verify_etl_process(
    bauplan_api_key: str = None,
    ) -> bool:
    """
    Verify the correctness of the ETL process by checking the state of the lakehouse.
    We return True if and only if all the tables are in the main branch and they have data inside them.
    
    Note that we make this function completely self-contained, with its own imports and a fresh Bauplan client.
    """
    
    import bauplan
    import pyarrow as pa
    # this assumed the process can access the usual Bauplan local config file
    # or that the proper environment variables are set
    client = bauplan.Client(api_key=bauplan_api_key)
    branch = 'main'
    # each table corresponds to a file loaded in the raw S3 bucket
    tables = [
            'acquirer_countries', 
            'payments', 
            'merchant_category_codes',
            'fees', 
            'merchant_data'
        ]
    # check for existence and for some data in each table
    try:
        for table in tables:
            assert client.has_table(table, ref=branch), f"Table {table} does not exist in the lakehouse"
            # we issue a query to check if the table has data and multiple columns
            query = f"SELECT * FROM {table} LIMIT 3"
            results: pa.Table = client.query(query, ref=branch)
            assert len(results.column_names) > 1, f"Table {table} does not have multiple columns."
            assert results.num_rows > 0, f"Table {table} is empty."
            print(f"Table {table} exists and has {results.num_rows} rows.")
    except AssertionError as e:
        print(f"Verification failed: {e}")
        return False

    return True