from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import CreateCluster, ClusterSpec

# Initialize Databricks workspace client
client = WorkspaceClient(
    host="REDACTED_YOUR_DATABRICKS_HOST",
    token="REDACTED_YOUR_DATABRICKS_TOKEN"
)

# Test connection
workspace = client.workspace.get_status("/")
print(f"Connected to workspace: {workspace.path}")

# Example: List clusters
clusters = client.clusters.list()
for cluster in clusters:
    print(f"Cluster: {cluster.cluster_name}")