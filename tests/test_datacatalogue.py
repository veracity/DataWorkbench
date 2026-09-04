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


VIEW_NAME = "receiver_cat.default.shared_view"
SOURCE_DATASET_ID = "11111111-1111-1111-1111-111111111111"
VIEW_ROWS = [{"table_type": "VIEW"}]
TAG_ROWS = [{"tag_value": SOURCE_DATASET_ID}]


def base_rows(table_type):
    return [{
        "table_catalog": "source_cat",
        "table_schema": "default",
        "table_name": "sales",
        "table_type": table_type,
    }]


def spark_returns(*result_sets):
    """One mocked spark.sql(...).collect() result per successive call."""
    return [MagicMock(collect=MagicMock(return_value=rows)) for rows in result_sets]


@pytest.fixture
def resolver():
    """DataCatalogue with a mocked Spark session for base table resolution."""
    with patch("dataworkbench.auth.TokenManager.get_token", return_value="mock_token"):
        catalogue = DataCatalogue()
    catalogue.storage = MagicMock()
    return catalogue


@pytest.mark.parametrize("view_name", ["", 123, None])
def test_resolve_base_table_invalid_view_name_type(resolver, view_name):
    with pytest.raises(TypeError):
        resolver.resolve_base_databricks_full_table_name(view_name)


@pytest.mark.parametrize("view_name", ["shared_view", "default.shared_view", "a.b.c.d", "cat..view"])
def test_resolve_base_table_not_fully_qualified(resolver, view_name):
    with pytest.raises(ValueError, match="View is not valid"):
        resolver.resolve_base_databricks_full_table_name(view_name)


@pytest.mark.parametrize("view_rows", [[], [{"table_type": "MATERIALIZED_VIEW"}], [{"table_type": "EXTERNAL"}]])
def test_resolve_base_table_input_must_be_a_plain_view(resolver, view_rows):
    resolver.storage.spark.sql.side_effect = spark_returns(view_rows)

    with pytest.raises(ValueError, match="View is not valid"):
        resolver.resolve_base_databricks_full_table_name(VIEW_NAME)


def test_resolve_base_table_without_source_dataset_id_tag(resolver):
    resolver.storage.spark.sql.side_effect = spark_returns(VIEW_ROWS, [])

    with pytest.raises(ValueError, match="doesn't have share with Write access on it"):
        resolver.resolve_base_databricks_full_table_name(VIEW_NAME)


def test_resolve_base_table_no_base_table_found(resolver):
    resolver.storage.spark.sql.side_effect = spark_returns(VIEW_ROWS, TAG_ROWS, [])

    with pytest.raises(ValueError, match="no base table found for this view"):
        resolver.resolve_base_databricks_full_table_name(VIEW_NAME)


@pytest.mark.parametrize("table_type", ["VIEW", "MATERIALIZED_VIEW", "MANAGED", "STREAMING_TABLE"])
def test_resolve_base_table_base_is_not_external(resolver, table_type):
    resolver.storage.spark.sql.side_effect = spark_returns(VIEW_ROWS, TAG_ROWS, base_rows(table_type))

    with pytest.raises(ValueError, match="is not an external table"):
        resolver.resolve_base_databricks_full_table_name(VIEW_NAME)


@pytest.mark.parametrize("view_name", [VIEW_NAME, " `receiver_cat`.`default`.`shared_view` "])
def test_resolve_base_table_returns_external_table_full_name(resolver, view_name):
    resolver.storage.spark.sql.side_effect = spark_returns(VIEW_ROWS, TAG_ROWS, base_rows("EXTERNAL"))

    result = resolver.resolve_base_databricks_full_table_name(view_name)

    assert result == "`source_cat`.`default`.`sales`"


def test_resolve_base_table_escapes_backticks_in_identifiers(resolver):
    quirky = [{
        "table_catalog": "source_cat",
        "table_schema": "default",
        "table_name": "we`ird",
        "table_type": "EXTERNAL",
    }]
    resolver.storage.spark.sql.side_effect = spark_returns(VIEW_ROWS, TAG_ROWS, quirky)

    result = resolver.resolve_base_databricks_full_table_name(VIEW_NAME)

    assert result == "`source_cat`.`default`.`we``ird`"


def test_resolve_base_table_never_interpolates_the_view_name(resolver):
    resolver.storage.spark.sql.side_effect = spark_returns(VIEW_ROWS, TAG_ROWS, base_rows("EXTERNAL"))

    resolver.resolve_base_databricks_full_table_name(VIEW_NAME)

    for call in resolver.storage.spark.sql.call_args_list:
        query = call.args[0]
        assert "receiver_cat" not in query
        assert SOURCE_DATASET_ID not in query
        assert call.kwargs["args"]


def test_resolve_base_table_passes_identifiers_to_args_unescaped(resolver):
    # Spark binds args as literals, so a quote must reach it verbatim -- escaping it
    # the way an interpolated WHERE clause would need is what breaks the match.
    resolver.storage.spark.sql.side_effect = spark_returns(VIEW_ROWS, TAG_ROWS, base_rows("EXTERNAL"))

    resolver.resolve_base_databricks_full_table_name("receiver_cat.default.o'brien_view")

    args = resolver.storage.spark.sql.call_args_list[0].kwargs["args"]
    assert args["view"] == "o'brien_view"
    assert "\\'" not in args["view"]

