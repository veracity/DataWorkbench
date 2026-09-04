import uuid
from enum import Enum
from typing import Any

from pyspark.sql import DataFrame

from dataworkbench.utils import get_secret, SparkDataFrame
from dataworkbench.storage import DeltaStorage
from dataworkbench.gateway import Gateway

from dataworkbench.log import setup_logger

# Configure logging
logger = setup_logger(__name__)


class WriteMode(Enum):
    """Enum representing the write modes for data storage."""

    OVERWRITE = "overwrite"


class DataCatalogue:
    """
    DataCatalogue service for storing and registering datasets to the dataworkbench.

    This class provides functionality to save data to a Delta Lake storage
    and register it with the data catalog via the Gateway service.
    """

    def __init__(self) -> None:
        """
        Initialize the Datacatalogue service with required dependencies.

        Sets up the storage writer, gateway client, and loads necessary
        configuration from secrets.
        """
        self.storage: DeltaStorage = DeltaStorage()
        self.gateway: Gateway = Gateway()
        self.storage_base_url: str = get_secret("StorageBaseUrl")

    def __build_storage_table_root_url(self, folder_id: uuid.UUID) -> str:
        """
        Build the ABFSS URL for the root location of the table
        """
        if not isinstance(folder_id, uuid.UUID):
            raise TypeError("folder_id must be uuid")

        return f"{self.storage_base_url}/{folder_id}"

    def __build_storage_table_processed_url(self, folder_id: uuid.UUID) -> str:
        """
        Build the ABFSS URL for the processed table storage location.

        Args:
            folder_id: Unique identifier for the storage folder

        Returns:
            str: Fully qualified ABFSS URL for the storage location

        Example:
            >>> catalogue = DataCatalogue()
            >>> catalogue.__build_storage_table_processed_url("abc123")
        """
        table_root_url = self.__build_storage_table_root_url(folder_id)
        return f"{table_root_url}/Processed"

    def save(
        self,
        df: DataFrame,
        dataset_name: str,
        dataset_description: str,
        schema_id: uuid.UUID | None = None,
        tags: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """
        Save a dataset to storage and register it with the data catalog.

        This method performs two operations:
        1. Writes the DataFrame to Delta Lake storage
        2. Registers the dataset metadata with the Gateway service

        Args:
            df: Spark DataFrame containing the dataset to be saved
            dataset_name: Name of the dataset for the catalog
            dataset_description: Description of the dataset's purpose and contents
            schema_id: Optional UUID for the dataset's schema
            tags: Optional metadata tags as key-value pairs

        Returns:
            Dict[str, Any]: Response from the Gateway service containing dataset
                            registration details or error information

        Raises:
            TypeError: If parameters are not of the expected types
            ValueError: If parameters fail validation checks

        Example:
            >>> datacatalogue = DataCatalogue()
            >>> df = spark.createDataFrame([("Alice", 30), ("Bob", 40)], ["name", "age"])
            >>> result = datacatalogue.save(
            ...     df,
            ...     "Customer Data",
            ...     "Monthly customer demographic data",
            ...     "550e8400-e29b-41d4-a716-446655440000",
            ...     {"environment": ["test"]}
            ... )
        """
        # Validate input parameters
        if not isinstance(df, SparkDataFrame):
            raise TypeError("df must be a DataFrame")

        if not isinstance(dataset_name, str) or not dataset_name:
            raise TypeError("dataset_name must be a non-empty string")

        if not isinstance(dataset_description, str):
            raise TypeError("dataset_description must be a string")

        if tags is not None and not isinstance(tags, dict):
            raise TypeError("tags must be a dictionary or None")

        # Generate folder_id
        folder_id = uuid.uuid4()

        target_path = self.__build_storage_table_processed_url(folder_id)

        try:
            # Write data using the specified or defaulted mode
            self.storage.write(df, target_path, mode=WriteMode.OVERWRITE.value)

            try:
                # Register the dataset with the Gateway API
                return self.gateway.import_dataset(
                    dataset_name, dataset_description, schema_id, tags or {}, folder_id
                )
            except Exception as e:
                self._rollback_write(folder_id)

                # Raise the original API error with additional context
                error_msg = (
                    f"Gateway API call failed and storage was rolled back: {str(e)}"
                )
                raise type(e)(error_msg) from e

        except Exception as e:
            return {"error": str(e), "error_type": type(e).__name__}

    def resolve_base_databricks_full_table_name(self, view_name: str) -> str:
        """
        Resolve the base table a write-shared view ultimately reads from.

        A view shared with write access carries a ``source_dataset_id`` tag holding the id
        of the root dataset. This method reads that tag and resolves the Unity Catalog
        object tagged ``dataset_id`` with the same value, which must be an external table.

        Args:
            view_name: Fully qualified view name, ``catalog.schema.view``. Must be a
                plain view; a materialized view is rejected.

        Returns:
            str: Fully qualified name of the base external table, with each identifier
                backtick quoted so the result can be used directly in Spark SQL

        Raises:
            TypeError: If view_name is not a non-empty string
            ValueError: If the view is not valid, does not carry a ``source_dataset_id``
                        tag, or its base object is missing or is not an external table

        Example:
            >>> catalogue = DataCatalogue()
            >>> catalogue.resolve_base_databricks_full_table_name(
            ...     "receiver_cat.default.shared_view"
            ... )
            '`source_cat`.`default`.`sales_2024`'
        """
        if not isinstance(view_name, str) or not view_name:
            raise TypeError("view_name must be a non-empty string")

        parts = [part.strip().strip("`") for part in view_name.split(".")]
        if len(parts) != 3 or not all(parts):
            raise ValueError("View is not valid")

        catalog, schema, view = parts
        spark = self.storage.spark
        view_args = {"catalog": catalog, "schema": schema, "view": view}

        view_rows = spark.sql(
            """
            SELECT table_type FROM system.information_schema.tables
            WHERE table_catalog = :catalog
              AND table_schema = :schema
              AND table_name = :view
            """,
            args=view_args,
        ).collect()

        if not view_rows or view_rows[0]["table_type"] != "VIEW":
            raise ValueError("View is not valid")

        tag_rows = spark.sql(
            """
            SELECT tag_value FROM system.information_schema.table_tags
            WHERE catalog_name = :catalog
              AND schema_name = :schema
              AND table_name = :view
              AND tag_name = 'source_dataset_id'
            """,
            args=view_args,
        ).collect()

        source_dataset_id = tag_rows[0]["tag_value"] if tag_rows else None
        if not source_dataset_id:
            raise ValueError("this view doesn't have share with Write access on it")

        logger.info(
            "Resolving base table for view %s via source_dataset_id %s",
            view_name,
            source_dataset_id,
        )

        # The base table lives in the source workspace's catalog, so this lookup is
        # deliberately not scoped to the catalog the view was found in.
        base_rows = spark.sql(
            """
            SELECT t.table_catalog, t.table_schema, t.table_name, t.table_type
            FROM system.information_schema.table_tags tg
            JOIN system.information_schema.tables t
              ON t.table_catalog = tg.catalog_name
             AND t.table_schema = tg.schema_name
             AND t.table_name = tg.table_name
            WHERE tg.tag_name = 'dataset_id'
              AND tg.tag_value = :dataset_id
            """,
            args={"dataset_id": source_dataset_id},
        ).collect()

        if not base_rows:
            raise ValueError("no base table found for this view")

        for row in base_rows:
            if row["table_type"] == "EXTERNAL":
                parts = (row["table_catalog"], row["table_schema"], row["table_name"])
                # Unity Catalog escapes a backtick inside an identifier by doubling it.
                return ".".join(f"`{part.replace('`', '``')}`" for part in parts)

        raise ValueError(
            "the base for this view is not an external table. Invalid viewName given as input"
        )

    def _rollback_write(self, folder_id: uuid.UUID) -> None:
        """
        Delete table from storage to rollback changes when an operation fails.

        Args:
            target_path: Path to the data in storage that should be deleted
        """
        target_path = self.__build_storage_table_root_url(folder_id)
        logger.info("Rolling back data write operation to storage")
        try:
            self.storage.delete(target_path, recursive=True)
        except Exception as rollback_error:
            logger.error(
                f"Failed to rollback storage operation at {target_path}: {str(rollback_error)}"
            )
            raise

        logger.info(
            f"Successfully rolled back data write operation by deleting: {target_path}"
        )
