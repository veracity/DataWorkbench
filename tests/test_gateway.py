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

    validation_message = "Either SchemaVersionId or SchemaId must be provided. Schemas are not created on import."
    trace_id = "8b01e7eb14484611add6138618daf112"
    response_body = {
        "type": "ValidationError",
        "title": "One or more validation errors occurred.",
        "status": 400,
        "detail": f"Validation failed: -- SchemaVersionId: {validation_message}",
        "errors": {"SchemaVersionId": [validation_message]},
        "traceId": trace_id,
    }

    mock_response = MagicMock()
    mock_response.return_value.status_code = 400
    mock_response.text = json.dumps(response_body)

    http_error = requests.exceptions.HTTPError()
    http_error.response = mock_response

    mock_response.raise_for_status.side_effect = http_error
    mock_post.return_value = mock_response

    with pytest.raises(RequestException) as e:
        mock_gateway.import_dataset("dataset_name", "dataset_description", None, {"tag": "value"}, "folder_id")

    assert validation_message in e.value.args[0]
    assert f"correlation-id: {trace_id}" in e.value.args[0]
    mock_post.assert_called_once()


def _failing_response(mock_post, response_body):
    mock_response = MagicMock()
    mock_response.text = json.dumps(response_body)
    http_error = requests.exceptions.HTTPError()
    http_error.response = mock_response
    mock_response.raise_for_status.side_effect = http_error
    mock_post.return_value = mock_response


def test_import_dataset_failure_without_detail_uses_errors(mock_gateway, mock_post):
    """Without `detail`, all field/message entries from `errors` are reported."""
    trace_id = "trace-123"
    _failing_response(
        mock_post,
        {
            "errors": {
                "SchemaVersionId": ["first problem", "second problem"],
                "Name": ["name problem"],
            },
            "traceId": trace_id,
        },
    )

    with pytest.raises(RequestException) as e:
        mock_gateway.import_dataset("n", "d", None, {"tag": "value"}, "folder_id")

    message = e.value.args[0]
    assert "SchemaVersionId: first problem" in message
    assert "SchemaVersionId: second problem" in message
    assert "Name: name problem" in message
    assert f"correlation-id: {trace_id}" in message


def test_import_dataset_failure_trace_only(mock_gateway, mock_post):
    """A response with only a trace ID still reports the correlation ID."""
    trace_id = "trace-456"
    _failing_response(mock_post, {"traceId": trace_id})

    with pytest.raises(RequestException) as e:
        mock_gateway.import_dataset("n", "d", None, {"tag": "value"}, "folder_id")

    assert f"correlation-id: {trace_id}" in e.value.args[0]
