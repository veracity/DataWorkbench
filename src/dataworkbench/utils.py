import os
from pyspark.sql import SparkSession

def get_spark() -> SparkSession:
    """
    Function that gets a spark session, checking if connected to databricks or not.

    returns: SparkSession
    """
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        return SparkSession.builder.getOrCreate()

def is_databricks():
    """
    Check if the code is running on Databricks.
    """
    return os.getenv("DATABRICKS_RUNTIME_VERSION") is not None

def get_secret(key: str, scope: str = "secrets") -> str:
    """
    Retrieve a secret from dbutils if running on Databricks, otherwise fallback to env variables.
    """
    
    secret = None  # Default value

    if is_databricks():
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            spark = get_spark()
            dbutils = DBUtils(spark)
            secret = dbutils.secrets.get(scope, key)
        except ImportError:
            raise RuntimeError("dbutils module not found. Ensure this is running on Databricks.")
    else:
        secret = os.getenv(key)

    # Raise an error if the secret is empty or None
    if not secret:
        raise ValueError(f"Secret '{key}' is missing or empty.")

    return secret

# Example usage
if __name__ == "__main__":
    CLIENT_ID = get_secret("ClientId")
    CLIENT_SECRET = get_secret("ClientSecret")