import pytest
import requests
from unittest.mock import patch, MagicMock
from dataworkbench.gateway import Gateway
from requests.exceptions import RequestException
import json

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

    response_body = {"type":"BusinessError","traceId":"8b01e7eb14484611add6138618daf112"}

    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = json.dumps(response_body)

    http_error = requests.exceptions.HTTPError()
    http_error.response = mock_response

    mock_response.raise_for_status.side_effect = http_error
    mock_post.return_value = mock_response

    with pytest.raises(RequestException) as e:
        mock_gateway.import_dataset("dataset_name", "dataset_description", "schema_id", {"tag": "value"}, "folder_id")

    assert response_body["traceId"] in e.value.args[0]
    mock_post.assert_called_once()


def _failing_post(mock_post, *, text, status_code=400):
    """Point requests.post at a response that raises HTTPError carrying `text`."""
    response = MagicMock()
    response.status_code = status_code
    response.text = text

    http_error = requests.exceptions.HTTPError()
    http_error.response = response
    response.raise_for_status.side_effect = http_error
    mock_post.return_value = response
    return response


def _import(gateway):
    return gateway.import_dataset(
        "dataset_name", "dataset_description", "schema_id", {"tag": "value"}, "folder_id"
    )


def test_import_dataset_failure_surfaces_problem_detail(mock_gateway, mock_post):
    """The `detail` the API explains the failure with must reach the caller."""
    _failing_post(mock_post, text=json.dumps({
        "title": "BadRequest",
        "status": 400,
        "detail": "Ensure DatasetName is unique when creating Predefined Dataset.",
        "traceId": "abc123",
    }))

    with pytest.raises(RequestException) as e:
        _import(mock_gateway)

    assert "Ensure DatasetName is unique when creating Predefined Dataset." in e.value.args[0]
    assert "abc123" in e.value.args[0]


def test_import_dataset_failure_surfaces_validation_errors(mock_gateway, mock_post):
    """Per-field validation errors are the actionable part of a 400."""
    _failing_post(mock_post, text=json.dumps({
        "title": "BadRequest",
        "status": 400,
        "errors": {"datasetName": ["must not be empty", "must be unique"]},
        "traceId": "def456",
    }))

    with pytest.raises(RequestException) as e:
        _import(mock_gateway)

    assert "datasetName" in e.value.args[0]
    assert "must not be empty" in e.value.args[0]
    # The summary and the field errors belong on separate lines so the message stays readable.
    assert "BadRequest\ndatasetName: must not be empty, must be unique" in e.value.args[0]


def test_import_dataset_failure_falls_back_to_title(mock_gateway, mock_post):
    """A body with no `detail` should still say something better than the trace id."""
    _failing_post(mock_post, text=json.dumps({"title": "Conflict", "status": 409, "traceId": "ghi789"}))

    with pytest.raises(RequestException) as e:
        _import(mock_gateway)

    assert "Conflict" in e.value.args[0]


def test_import_dataset_failure_with_non_json_body(mock_gateway, mock_post):
    """A gateway/proxy can answer with HTML; parsing it must not mask the real error."""
    _failing_post(mock_post, text="<html><body>502 Bad Gateway</body></html>", status_code=502)

    with pytest.raises(RequestException) as e:
        _import(mock_gateway)

    assert "502" in e.value.args[0]


def test_import_dataset_failure_without_response(mock_gateway, mock_post):
    """A connection error has no response at all."""
    mock_post.side_effect = requests.exceptions.ConnectionError("connection refused")

    with pytest.raises(RequestException) as e:
        _import(mock_gateway)

    assert "Failed to create data catalog entry" in e.value.args[0]
