from typing import Literal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException
from abc import ABC, abstractmethod
from dataworkbench.utils import get_dbutils, PrimitiveType, is_databricks

from dataworkbench.log import setup_logger


# Configure logging
logger = setup_logger(__name__)


class Storage(ABC):
    """
    Abstract base class for storage writers.

    Defines the interface for all storage writer implementations.
    Specific storage implementations should inherit from this class
    and implement the write method.
    """

    @abstractmethod
    def write(
        self,
        df: DataFrame,
        target_path: str,
        mode: Literal["overwrite", "append", "error", "ignore"] = "overwrite",
        **options: PrimitiveType | None,
    ) -> None:
        """
        Write a DataFrame to storage.

        Args:
            df: The DataFrame to write
            target_path: The destination path where data should be written
            mode: The write mode to use
            **options: Additional options to pass to the writer

        Returns:
            None

        Raises:
            NotImplementedError: If the subclass does not implement this method
        """
        pass

    @abstractmethod
    def check_path_exists(self, path: str) -> bool:
        """
        Check if a path exists in storage.

        Args:
            path: The path to check for existence

        Returns:
            bool: True if the path exists, False otherwise

        Raises:
            NotImplementedError: If the subclass does not implement this method
        """
        pass

    @abstractmethod
    def read(self, source_path: str, **options: PrimitiveType | None) -> DataFrame:
        """
        Read data from storage into a DataFrame.

        Args:
            source_path: The source path to read data from
            **options: Additional options to pass to the reader

        Returns:
            DataFrame: The loaded data

        Raises:
            NotImplementedError: If the subclass does not implement this method
        """
        pass


class DeltaStorage(Storage):
    """
    Writes and reads data in Delta format in storage.

    This class handles writing Spark DataFrames to Delta Lake format
    with configurable write modes and error handling, as well as
    reading Delta tables and checking path existence.
    """

    def __init__(self, spark_session: SparkSession | None = None):
        """
        Initialize the Delta Storage Writer.

        Args:
            spark_session: Optional SparkSession to use for operations.
                           If not provided, will use the active session.
        """
        if spark_session is not None and not isinstance(spark_session, SparkSession):
            raise TypeError("spark_session must be a SparkSession or None")

        self._spark = spark_session
        self._dbutils = get_dbutils(self._spark)

    @property
    def spark(self) -> SparkSession:
        """
        Get the SparkSession for this writer.

        Returns:
            SparkSession: The current SparkSession

        Raises:
            RuntimeError: If no SparkSession is available
        """
        if self._spark is None:
            try:
                # Get active SparkSession
                from pyspark.sql import SparkSession

                self._spark = SparkSession.builder.getOrCreate()
            except Exception as e:
                logger.error(f"Failed to create SparkSession: {e}")
                raise RuntimeError("No SparkSession available") from e

        return self._spark

    def write(
        self,
        df: DataFrame,
        target_path: str,
        mode: Literal["overwrite", "append", "error", "ignore"] = "overwrite",
        partition_by: str | list[str] | None = None,
        **options: PrimitiveType | None,
    ) -> None:
        """
        Write a DataFrame to storage in Delta format.

        Args:
            df: The DataFrame to write to storage
            target_path: The destination path where data should be written
            mode: The write mode to use. Options are:
                  - 'overwrite': Overwrite existing data
                  - 'append': Append to existing data
                  - 'error': Throw error if data exists
                  - 'ignore': Silently ignore if data exists
            **options: Additional options to pass to the Delta writer

        Returns:
            None

        Raises:
            TypeError: If parameters are not of the expected types
            ValueError: If parameters fail validation
            RuntimeError: If the DataFrame cannot be written to storage

        Example:
            >>> storage = DeltaStorage()
            >>> df = spark.createDataFrame([("Alice", 30), ("Bob", 40)], ["name", "age"])
            >>> storage.write(df, "abfss://container@account.dfs.core.windows.net/path/to/data")
        """
        # Validate inputs
        if not hasattr(df, "write"):
            raise TypeError("df must have write attribute")

        if not isinstance(target_path, str) or not target_path:
            raise TypeError("target_path must be a non-empty string")

        if mode not in ["overwrite", "append", "error", "ignore"]:
            raise ValueError(
                f"Invalid mode: {mode}. Must be one of: overwrite, append, error, ignore"
            )

        try:
            logger.info(f"Writing DataFrame to: {target_path} with mode: {mode}")
            writer = df.write.format("delta").mode(mode)

            # Apply options if provided
            if options:
                writer = writer.options(**options)

            if partition_by:
                writer = writer.partitionBy(partition_by)

            # Save the data
            writer.save(target_path)
            logger.info(f"Successfully wrote data to {target_path}")

        except Exception as e:
            error_msg = f"Failed to write data to storage at {target_path}: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def append(
        self,
        df: DataFrame,
        target_path: str,
        partition_by: str | list[str] | None = None,
        **options: PrimitiveType | None,
    ) -> None:
        """
        Append a DataFrame to existing data in Delta format.

        This is a convenience method that calls write() with mode='append'.

        Args:
            df: The DataFrame to append to storage
            target_path: The destination path where data should be appended
            partition_by: Optional column(s) to partition the data by
            **options: Additional options to pass to the Delta writer

        Returns:
            None

        Raises:
            RuntimeError: If the DataFrame cannot be appended to storage

        Example:
            >>> storage = DeltaStorage()
            >>> new_records = spark.createDataFrame([("Charlie", 35)], ["name", "age"])
            >>> storage.append(new_records, "abfss://container@account.dfs.core.windows.net/path/to/data")
        """
        self.write(
            df=df,
            target_path=target_path,
            mode="append",
            partition_by=partition_by,
            **options,
        )

    def check_path_exists(self, path: str) -> bool:
        """
        Check if a path exists in the storage and contains a valid Delta table.

        Args:
            path: The path to check for existence

        Returns:
            bool: True if the path exists and contains a valid Delta table, False otherwise

        Raises:
            TypeError: If path is not a string

        Example:
            >>> storage = DeltaStorage()
            >>> exists = storage.check_path_exists("abfss://container@account.dfs.core.windows.net/path/to/data")
        """
        if not isinstance(path, str) or not path:
            raise TypeError("path must be a non-empty string")

        try:
            # Try to read the delta table metadata to check if it exists
            self.spark.read.format("delta").load(path).limit(1).count()
            return True
        except AnalysisException:
            # Path doesn't exist or isn't a valid Delta table
            logger.debug(f"Path does not exist or is not a valid Delta table: {path}")
            return False
        except Exception as e:
            logger.warning(f"Error checking path existence: {e}")
            return False

    def read(self, source_path: str, **options: PrimitiveType | None) -> DataFrame:
        """
        Read a Delta table from storage into a DataFrame.

        Args:
            source_path: The source path to read data from
            **options: Additional options to pass to the Delta reader

        Returns:
            DataFrame: The loaded data

        Raises:
            TypeError: If source_path is not a string
            RuntimeError: If the data cannot be read from storage

        Example:
            >>> storage = DeltaStorage()
            >>> df = storage.read("abfss://container@account.dfs.core.windows.net/path/to/data")
        """
        if not isinstance(source_path, str) or not source_path:
            raise TypeError("source_path must be a non-empty string")

        try:
            logger.info(f"Reading Delta table from: {source_path}")
            reader = self.spark.read.format("delta")

            # Apply options if provided
            if options:
                reader = reader.options(**options)

            # Load the data
            return reader.load(source_path)

        except Exception as e:
            error_msg = f"Failed to read data from {source_path}: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def file_exists(self, path: str):
        if is_databricks():
            try:
                self._dbutils.fs.ls(path)
                return True
            except Exception as e:
                if "java.io.FileNotFoundException" in str(e):
                    return False
                else:
                    raise
        else:
            logger.info("This method is not implemented outside databricks")

    def delete(self, path: str, recursive: bool = True) -> None:
        """
        Delete a directory from Azure Storage using Spark.

        Args:
            path: The path to the file / directory in Azure Storage to delete
            recursive: If True, recursively delete all subdirectories and files

        Raises:
            TypeError: If path is not a string
            ValueError: If path is empty
            Exception: If any error occurs during deletion
        """
        if not is_databricks():
            raise RuntimeError("Delete does not work outside databricks")

        if not isinstance(path, str):
            raise TypeError("path must be a non-empty string")

        if not path:
            raise ValueError("path cannot be empty")

        try:
            logger.info(f"Deleting path: {path}, recursive={recursive}")

            if not self.file_exists(path):
                logger.warning(f"Path does not exist, nothing to delete: {path}")
                return

            # Delete the path
            self._dbutils.fs.rm(path, recurse=True)

        except Exception as e:
            logger.error(f"Failed to delete {path}: {str(e)}")
            raise Exception(f"Failed to delete: {str(e)}") from e
