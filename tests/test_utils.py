import os
import pytest
from unittest.mock import patch, MagicMock
from dataworkbench.utils import is_databricks, get_secret  # Adjust import based on module name

# ------------------ FIX: Mock SparkSession globally ------------------
@pytest.fixture(scope="session", autouse=True)
def mock_spark():
    """Prevent PySpark from launching by mocking SparkSession globally."""
    with patch("pyspark.sql.SparkSession") as mock_spark_cls:
        mock_spark_instance = MagicMock()
        mock_spark_cls.builder.getOrCreate.return_value = mock_spark_instance
        yield mock_spark_instance  # Provide mock Spark instance to tests

# ------------------ TEST: is_databricks() ------------------
@patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "12.2"} )
def test_is_databricks_true():
    """Test is_databricks() when running on Databricks."""
    assert is_databricks() is True


@patch.dict(os.environ, {}, clear=True)
def test_is_databricks_false():
    """Test is_databricks() when NOT running on Databricks."""
    assert is_databricks() is False

@patch("dataworkbench.utils.is_databricks", return_value=False)
@patch.dict(os.environ, {}, clear=True)  # No env vars set
def test_get_secret_missing_env(mock_is_databricks):
    """Test get_secret() when key is missing from both Databricks and environment."""
    with pytest.raises(ValueError, match="Secret 'apiKey' is missing or empty."):
        get_secret("apiKey")
