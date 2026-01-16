import dagster as dg
import pandas as pd 
from databricks import sql
import os
from dotenv import load_dotenv


@dg.asset
def raw_patients() -> None:
    """Asset representing the landing patient data. (landing layer)
       Get the data from Databricks and save into parquet format.
    """
    # here is coming the Databricks logic to load patient data
    # Load environment variables from a .env file
    load_dotenv()
    
    # Retrieve Databricks connection details from environment variables
    server_hostname = os.getenv("DATABRICKS_HOST")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")
    
    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    ) as connection:
        with connection.cursor() as cursor:
            # Unity Catalog uses a 3-level namespace: catalog.schema.table
            cursor.execute("SELECT count(*) FROM workspace.default.bronze_customers where LIMIT 15")


            # Fetch as a Pandas DataFrame
            df = cursor.fetchall_arrow().to_pandas()
            
            # Write to parquet file
            output_dir = "data/raw/landing"
            os.makedirs(output_dir, exist_ok=True)
            df.to_parquet(f"{output_dir}/patients.parquet")


@dg.asset
def bronze_patients() -> None:
    """Asset representing the staging patient data. (bronze layer)"""
    # here is coming the inmemory logic to load patient data into DuckDB 
    pass


@dg.asset(deps=["bronze_patients"])
def silver_patients() -> None:
    """Asset representing the cleaned patient data. (silver layer)"""
    # here is coming the in memory logic to load patient data to the next layer in DuckDB
    pass



@dg.asset(deps=["silver_patients"])
def gold_patients() -> None:
    """Asset representing the curated patient data. (gold layer)"""
    # here is coming the in memory logic to load patient data to the next layer in DUckDB
    pass

