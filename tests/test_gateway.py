import pytest
import requests
from unittest.mock import patch, MagicMock
from dataworkbench.gateway import Gateway

@pytest.fixture
def mock_gateway():
    """Fixture to mock the Gateway instance."""
    with patch("dataworkbench.auth.TokenManager.get_token", return_value="mock_token"), \
         patch("dataworkbench.storage.DeltaStorage"), \
         patch("dataworkbench.gateway.Gateway"):

        gateway_instance = Gateway()
        return gateway_instance

@pytest.fixture
def mock_post():
    """Fixture to mock requests.post."""
    with patch("requests.post") as mock_request:
        yield mock_request

def test_import_dataset_success(mock_gateway, mock_post):
    """Test successful dataset import."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"status": "success"}
    mock_response.raise_for_status = MagicMock()
    mock_post.return_value = mock_response

    result = mock_gateway.import_dataset("dataset_name", "dataset_description", "schema_id", {"tag": "value"}, "folder_id")

    assert result == {"status": "success"}
    mock_post.assert_called_once()

def test_import_dataset_failure(mock_gateway, mock_post):
    """Test dataset import failure."""
    mock_post.side_effect = requests.exceptions.RequestException("Request failed")

    result = mock_gateway.import_dataset("dataset_name", "dataset_description", "schema_id", {"tag": "value"}, "folder_id")

    assert result == {"error": "Failed to create data catalog entry: Request failed"}
    mock_post.assert_called_once()
