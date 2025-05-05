import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import DataFrame
import uuid

from dataworkbench.storage import DeltaStorage
from dataworkbench.datacatalogue import DataCatalogue
from dataworkbench.gateway import Gateway

from requests.exceptions import RequestException


@pytest.fixture
def mock_dependencies():
    """Patch all external dependencies and return a mock BYOD instance"""
    with patch("dataworkbench.storage.DeltaStorage") as MockStorage, \
         patch("dataworkbench.gateway.Gateway") as MockGateway, \
         patch("dataworkbench.auth.TokenManager.get_token", return_value="mock_token"):

        datacatalogue = DataCatalogue()
        return datacatalogue, MockStorage.return_value, MockGateway.return_value

@pytest.fixture
def storage_handler():
    handler = DataCatalogue()
    handler.storage = MagicMock()
    handler._DataCatalogue__build_storage_table_root_url = MagicMock()

    return handler


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


@pytest.mark.parametrize("folder_id", ["", 123, "5f69754e-37a0-431b-aa3e-3f5e361017fa"])
def test_invalid_folder_id_build_storage_table_root_url(mock_dependencies, folder_id):
    datacatalogue, _, _ = mock_dependencies
    with pytest.raises(TypeError):
        datacatalogue._DataCatalogue__build_storage_table_root_url(folder_id)



def test_save_dataset_invalid_df(mock_dependencies):
    datacatalogue, _, _ = mock_dependencies
    df = "a string"
    with pytest.raises(TypeError):
        datacatalogue.save(df, "name", "description")


@pytest.mark.parametrize("name", ["", 123])
def test_save_dataset_invalid_name(mock_dependencies, name):
    datacatalogue, _, _ = mock_dependencies
    with pytest.raises(TypeError):
        datacatalogue.save(
            df=MagicMock(spec=DataFrame),
            dataset_name=name,
            dataset_description="test description"
        )

def test_save_dataset_invalid_description(mock_dependencies):
    datacatalogue, _, _ = mock_dependencies
    description = 123
    with pytest.raises(TypeError):
        datacatalogue.save(
            df=MagicMock(spec=DataFrame),
            dataset_name="name",
            dataset_description=description
        )

@pytest.mark.parametrize("tags", ["tags: test", "{tags: test}", 123])
def test_save_dataset_invalid_tags(mock_dependencies, tags):
    datacatalogue, _, _ = mock_dependencies
    with pytest.raises(TypeError):
        datacatalogue.save(
            df=MagicMock(spec=DataFrame),
            dataset_name="name",
            dataset_description="description",
            tags=tags
        )


def test_save_gateway_failure_triggers_rollback(mock_dependencies, storage_handler):

    folder_id = uuid.uuid4()
    target_path = f".../{folder_id}"
    datacatalogue, _, _ = mock_dependencies

    datacatalogue.gateway.import_dataset = MagicMock()
    datacatalogue.gateway.import_dataset.side_effect = RequestException()

    storage_handler._DataCatalogue__build_storage_table_root_url.return_value = target_path

    datacatalogue._rollback_write = MagicMock()

    result = datacatalogue.save(
        df=MagicMock(spec=DataFrame),
        dataset_name="name",
        dataset_description="description"
    )

    assert "error" in result
    assert "error_type" in result

    datacatalogue.gateway.import_dataset.assert_called_once()
    datacatalogue._rollback_write.assert_called_once()


def test_save_gateway_failure_and_rollback_fails(mock_dependencies, storage_handler):
    folder_id = uuid.uuid4()
    target_path = f".../{folder_id}"
    datacatalogue, _, _ = mock_dependencies

    datacatalogue.gateway.import_dataset = MagicMock()
    datacatalogue.gateway.import_dataset.side_effect = RequestException()

    storage_handler._DataCatalogue__build_storage_table_root_url.return_value = target_path

    datacatalogue._rollback_write = MagicMock()
    error_msg = "some type of error"
    datacatalogue._rollback_write.side_effect = RuntimeError(error_msg)

    result = datacatalogue.save(
        df=MagicMock(spec=DataFrame),
        dataset_name="name",
        dataset_description="description"
    )

    assert "error" in result
    assert "error_type" in result

    assert result["error_type"] == "RuntimeError"
    assert result["error"] == error_msg

    datacatalogue.gateway.import_dataset.assert_called_once()
    datacatalogue._rollback_write.assert_called_once()


def test_rollback_write_success(storage_handler):
    folder_id = uuid.uuid4()
    target_path = f".../{folder_id}"
    storage_handler._DataCatalogue__build_storage_table_root_url.return_value = target_path

    storage_handler._rollback_write(folder_id)
    storage_handler.storage.delete.assert_called_once_with(target_path, recursive=True)



def test_rollback_write_delete_fails_logs_error(storage_handler):
    folder_id = uuid.uuid4()
    target_path = f".../{folder_id}"
    storage_handler._DataCatalogue__build_storage_table_root_url.return_value = target_path

    storage_handler.storage.delete.side_effect = Exception()

    with pytest.raises(Exception):
        storage_handler._rollback_write(folder_id)

    storage_handler.storage.delete.assert_called_once_with(target_path, recursive=True)
