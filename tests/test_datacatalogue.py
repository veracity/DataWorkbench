import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame

from dataworkbench.storage import DeltaStorage
from dataworkbench.datacatalogue import DataCatalogue
from dataworkbench.gateway import Gateway


@pytest.fixture
def mock_dependencies():
    """Patch all external dependencies and return a mock BYOD instance"""
    with patch("dataworkbench.storage.DeltaStorage") as MockStorage, \
         patch("dataworkbench.gateway.Gateway") as MockGateway, \
         patch("dataworkbench.auth.TokenManager.get_token", return_value="mock_token"):

        datacatalogue = DataCatalogue()
        return datacatalogue, MockStorage.return_value, MockGateway.return_value

@patch.object(DeltaStorage, "write", return_value="mock_write_success")
@patch.object(Gateway, "import_dataset", return_value="mock_datacatalog_success")
def test_save_dataset(mock_write, mock_gateway_import, mock_dependencies):
    """Test saving a dataset without making real API calls"""
    datacatalogue, _, _ = mock_dependencies

    result = datacatalogue.save(
        df=MagicMock(spec=DataFrame),
        dataset_name="test_dataset",
        dataset_description="test description"
    )

    assert result == "mock_datacatalog_success"
    mock_write.assert_called_once()
    mock_gateway_import.assert_called_once()
