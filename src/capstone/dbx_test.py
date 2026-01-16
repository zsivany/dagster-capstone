from databricks import sql
import pandas as pd
import os
from dotenv import load_dotenv
# Load environment variables from a .env file
load_dotenv()

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
        cursor.execute("SELECT * FROM workspace.default.bronze_customers LIMIT 15")

        # Fetch as a Pandas DataFrame
        df = cursor.fetchall_arrow().to_pandas()
        print(df.head(21))