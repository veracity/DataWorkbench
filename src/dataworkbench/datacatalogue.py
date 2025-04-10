import uuid
from enum import Enum
from typing import Any

from pyspark.sql import DataFrame

from dataworkbench.utils import get_secret
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

    def __build_storage_url(self, folder_id: str) -> str:
        """
        Build the ABFSS URL for the target storage location.

        Args:
            folder_id: Unique identifier for the storage folder

        Returns:
            str: Fully qualified ABFSS URL for the storage location

        Example:
            >>> catalogue = DataCatalogue()
            >>> catalogue._build_storage_url("abc123")
        """
        if not isinstance(folder_id, str):
            raise TypeError("folder_id must be a string")

        if not folder_id:
            raise ValueError("folder_id cannot be empty")

        return f"{self.storage_base_url}/{folder_id}/Processed"

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
        if not hasattr(df, "write"):
            raise TypeError("df must be a DataFrame")

        if not isinstance(dataset_name, str) or not dataset_name:
            raise TypeError("dataset_name must be a non-empty string")

        if not isinstance(dataset_description, str):
            raise TypeError("dataset_description must be a string")

        if tags is not None and not isinstance(tags, dict):
            raise TypeError("tags must be a dictionary or None")

        # Generate folder_id
        folder_id = str(uuid.uuid4())

        target_path = self.__build_storage_url(folder_id)

        try:
            # Write data using the specified or defaulted mode
            self.storage.write(df, target_path, mode=WriteMode.OVERWRITE.value)

            return self.gateway.import_dataset(
                dataset_name, dataset_description, schema_id, tags or {}, folder_id
            )

        except Exception as e:
            return {"error": str(e), "error_type": type(e).__name__}
