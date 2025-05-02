import os
from pyspark.sql import SparkSession, DataFrame

from dataworkbench.log import setup_logger

# Configure logging
logger = setup_logger(__name__)


PrimitiveType = str | int | float | bool


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


def get_dbutils(spark: SparkSession | None = None):
    """
    Get dbutils module
    """
    if is_databricks():
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
        except ImportError:
            raise RuntimeError(
                "dbutils module not found. Ensure this is running on Databricks."
            )
        try:
            return DBUtils(spark)
        except Exception as e:
            logger.error(f"Failed to create dbutils: {e}")
            raise RuntimeError("No dbutils available") from e
    else:
        return None


def get_secret(key: str, scope: str = "dwsecrets") -> str:
    """
    Retrieve a secret from dbutils if running on Databricks, otherwise fallback to env variables.
    """

    dbutils = get_dbutils()

    if dbutils:
        secret = dbutils.secrets.get(scope, key)
    else:
        secret = os.getenv(key)

    # Raise an error if the secret is empty or None
    if not secret:
        raise ValueError(f"Secret '{key}' is missing or empty.")

    return secret


if is_databricks():
    from pyspark.sql.connect.dataframe import DataFrame as DatabricksDataFrame

    SparkDataFrame = DataFrame | DatabricksDataFrame
else:
    SparkDataFrame = DataFrame


# Example usage
if __name__ == "__main__":
    CLIENT_ID = get_secret("ClientId")
    CLIENT_SECRET = get_secret("ClientSecret")
