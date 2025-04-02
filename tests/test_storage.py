import pytest
from unittest.mock import MagicMock
from pyspark.sql import DataFrame
from dataworkbench.storage import DeltaStorage

@pytest.fixture
def writer():
    """Fixture to initialize DeltaStorage."""
    return DeltaStorage()

@pytest.fixture
def mock_df():
    """Fixture to create a mock DataFrame with a writable attribute."""
    mock_df = MagicMock(spec=DataFrame)
    mock_write = MagicMock()
    
    # Simulate DataFrame.write property
    mock_df.write = mock_write  

    # Mock method chaining: format().mode().save()
    mock_write.format.return_value = mock_write
    mock_write.mode.return_value = mock_write

    return mock_df

def test_write_success(writer, mock_df):
    """Test successful write operation."""
    mock_target_path = "mock_path"

    writer.write(mock_df, mock_target_path)

    mock_df.write.format.assert_called_once_with("delta")
    mock_df.write.format().mode.assert_called_once_with("overwrite")
    mock_df.write.format().mode().save.assert_called_once_with(mock_target_path)

def test_write_failure(writer, mock_df):
    """Test write failure with exception handling."""
    mock_target_path = "mock_path"

    # Simulate a failure in the save() method
    mock_df.write.format().mode().save.side_effect = Exception("Write failed")

    with pytest.raises(RuntimeError, match="Failed to write data to storage."):
        writer.write(mock_df, mock_target_path)
