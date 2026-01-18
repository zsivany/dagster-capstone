import dagster as dg
import pandas as pd 
from databricks import sql
import os
from dotenv import load_dotenv
import duckdb
from .partitions import daily_partition
#from defs.assets.partitions import daily_partition
from dagster import build_asset_context


@dg.asset(partitions_def=daily_partition, group_name="raw_patients")

def raw_patients(context: dg.AssetExecutionContext) -> None:
    """Asset representing the landing patient data. (landing layer)
       Get the data from Databricks and save into parquet format.
    """
    # here is coming the Databricks logic to load patient data
    # Load environment variables from a .env file
    # Use explicit path to find .env from workspace root
    env_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env")
    load_dotenv(env_path)
    

    partition_date_str = context.partition_key
    print(partition_date_str)
    # Retrieve Databricks connection details from environment variables
    server_hostname = os.getenv("DATABRICKS_HOST")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")
    

    # Debug: Print loaded variables
    # print(f"DATABRICKS_HOST: {server_hostname}")
    # print(f"DATABRICKS_HTTP_PATH: {http_path}")
    # print(f"DATABRICKS_TOKEN: {access_token}")
    
    if not all([server_hostname, http_path, access_token]):
        raise ValueError("Missing Databricks environment variables. Check your .env file.")
    
    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    ) as connection:
        with connection.cursor() as cursor:
            # Unity Catalog uses a 3-level namespace: catalog.schema.table
            cursor.execute(f"""SELECT * FROM 
                           workspace.default.bronze_customers 
                           WHERE CAST(TO_TIMESTAMP(operation_date, 'MM-dd-yyyy HH:mm:ss') AS DATE) = DATE ('{partition_date_str}')
                           """)


            # Fetch as a Pandas DataFrame
            df = cursor.fetchall_arrow().to_pandas()
            
            print(df.head())
            print(df.count())
            # Write to parquet file
            output_dir = "data/raw/landing"
            os.makedirs(output_dir, exist_ok=True)
            df.to_parquet(f"{output_dir}/patients.parquet")


@dg.asset(deps=["raw_patients"], group_name="bronze_patients")
def bronze_patients() -> None:
    """Asset representing the staging patient data. (bronze layer)"""
    # here is coming the inmemory logic to load patient data into DuckDB 
    output_dir = "data/raw/landing/patients.parquet"
    conn = duckdb.connect("data.duckdb")
    
    conn.execute("""
    CREATE TABLE IF NOT EXISTS bronze_patients (
    address         VARCHAR,
    email           VARCHAR,
    firstname       VARCHAR,
    id              VARCHAR,
    lastname        VARCHAR,
    load_ts         VARCHAR,
    operation       VARCHAR,
    operation_date  VARCHAR,
    _rescued_data   VARCHAR
    ); """)

    print("bronze_patients table created if not exists")

    conn.execute(f"INSERT INTO bronze_patients SELECT * FROM '{output_dir}'")
    print("Data inserted into existing bronze_patients table")
    # # Check if table exists
    # result = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'bronze_patients'").fetchall()
    
    # if result[0][0] > 0:
    #     # Table exists, insert into it
    #     conn.execute(f"INSERT INTO bronze_patients SELECT * FROM '{output_dir}'")
    #     print("Data inserted into existing bronze_patients table")
    # else:
    #     # Table doesn't exist, create it
    #     conn.execute(f"CREATE TABLE bronze_patients AS SELECT * FROM '{output_dir}'")
    #     print("New bronze_patients table created")
    


@dg.asset(deps=["bronze_patients"], group_name="silver_patients")
def silver_patients() -> None:
    """Asset representing the cleaned patient data. (silver layer)"""
    # here is coming the in memory logic to load patient data to the next layer in DuckDB and casting columns
    query = """ --CREATE TABLE IF NOT EXISTS silver_patients AS
                INSERT INTO silver_patients
                SELECT address AS address,
                email AS email,
                firstname AS first_name,
                lastname AS last_name,
                concat(firstname, ' ', lastname) AS full_name,
                id AS patient_id,
                operation AS operation,
                CAST(
                    strptime(operation_date, '%m-%d-%Y %H:%M:%S')
                    AS DATE
                ) AS operation_date,
                CAST(
                    strptime(load_ts, '%Y-%m-%dT%H:%M:%S.%fZ')
                    AS TIMESTAMP
                ) AS load_timestamp
                FROM bronze_patients 
                --WHERE operation_date > '2023-01-01'"""
    
    conn = duckdb.connect("data.duckdb")
    conn.execute(query)
    print("silver_patients table inserted from bronze_patients")
    # df = conn.execute(query).fetchdf()
    # print(df)

@dg.asset(deps=["silver_patients"], group_name="gold_patients")
def gold_deleted_patients() -> None:
    """Asset representing the curated patient data. (gold layer)"""
    # here is coming the in memory logic to load patient data to the next layer in DUckDB
    query = """ --CREATE TABLE IF NOT EXISTS gold_deleted_patients AS
                INSERT INTO gold_deleted_patients
                SELECT                 
                patient_id,
                operation,
                operation_date,
                load_timestamp
                FROM silver_patients 
                WHERE 1 = 1
                --AND operation_date > '2023-01-01'
                AND operation = 'DELETED'"""
    
    conn = duckdb.connect("data.duckdb")
    conn.execute(query)
    print("gold_deleted_patients table inserted from silver_patients")
    # df = conn.execute(query).fetchdf()
    # print(df)

#Local test
context = build_asset_context(partition_key="2025-10-01")  # pass a partition if needed
raw_patients(context)
#bronze_patients()
# silver_patients()
# gold_deleted_patients()