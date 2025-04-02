import pytest
import requests
from unittest.mock import patch, MagicMock
from dataworkbench.auth import TokenManager


@pytest.fixture
def mock_post():
    """Mock requests.post to simulate API responses."""
    with patch("requests.post") as mock_request:
        yield mock_request

@pytest.fixture
def mock_secrets():
    """Mock get_secret function to return fake secrets."""
    with patch("dataworkbench.auth.get_secret") as mock_get_secret:
        mock_get_secret.side_effect = lambda key: {
            "ApimTokenUrl": "https://example.com/token",
            "ApimClientId": "test_client_id",
            "ApimClientSecret": "test_client_secret",
            "ApimScope": "test_scope"
        }.get(key, "dummy_value")
        yield mock_get_secret

def test_get_token_success(mock_secrets, mock_post):
    """Test if TokenManager.get_token() correctly retrieves a token."""
    # Mock successful API response
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = {"access_token": "test_token"}
    mock_post.return_value = mock_response

    # Call method
    token = TokenManager.get_token()

    # Assertions
    assert token == "test_token"
    mock_post.assert_called_once_with(
        "https://example.com/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "scope": "test_scope"
        }
    )

def test_get_token_failure(mock_secrets, mock_post, caplog):
    """Test if TokenManager.get_token() handles failure correctly."""
    # Mock API failure
    mock_post.side_effect = requests.exceptions.RequestException("Request failed")

    token = TokenManager.get_token()

    # Assertions
    assert token is None
