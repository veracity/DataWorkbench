# Databricks notebook source
import logging
import sys

# COMMAND ----------

from pyspark.sql import SparkSession

def get_spark() -> SparkSession:
  try:
    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.getOrCreate()
  except ImportError:
    return SparkSession.builder.getOrCreate()

# COMMAND ----------

logger = logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

spark = get_spark()

# COMMAND ----------
sys.path.insert(0, '../src/')

from dataworkbench import DataPublisher
publisher = DataPublisher()

# COMMAND ----------

# Create dataframe
df_test = spark.createDataFrame([("a", 1), ("b", 2), ("c", 3)], ["letter", "number"])

# Save to data workbench
publisher.save_dataset(df_test, "writeBackFromLocalDevNoSchema", "test_dataset_description", tags={"test": ["test"]})