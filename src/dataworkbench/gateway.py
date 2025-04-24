from typing import Any
import requests
from uuid import UUID
import json

from dataworkbench.utils import get_secret
from dataworkbench.auth import TokenManager
from dataworkbench.log import setup_logger

# Configure logging
logger = setup_logger(__name__)


def _get_trace_id_from_response(response: requests.Response) -> str | None:
    response_dict = json.loads(response.text)
    return response_dict.get("traceId")


class Gateway:
    """
    Handles interactions with the remote data gateway API.

    This class provides methods to communicate with the DataWorkbench Gateway,
    handling authentication, request formatting, and error handling.

    Public API:
    - import_dataset: Imports a dataset into the DataWorkbench

    All other methods are for internal use only and may change without notice.
    """

    def __init__(self) -> None:
        """
        Initialize the Gateway with configuration from secrets.

        Sets up the base URL, workspace ID, and request headers for API calls.
        """
        self.__gateway_url: str = get_secret("GatewayBaseUrl")
        self.__workspace_id: str = get_secret("DwbWorkspaceId")
        self.__token: str | None = None
        self.__request_headers: dict[str, str] = self.__setup_request_headers()

    def __setup_request_headers(self) -> dict[str, str]:
        """
        Set up the necessary headers for API requests.

        Private method that retrieves authentication token and subscription key from secrets.

        Returns:
            Dict[str, str]: Headers dictionary containing Content-Type, subscription key,
                            and authorization token.
        """
        self.__token = TokenManager.get_token()
        if not self.__token:
            logger.error("Failed to retrieve access token")
            return {"error": "Failed to retrieve access token."}

        return {
            "Content-Type": "application/json",
            "Ocp-Apim-Subscription-Key": get_secret("OcpApimSubscriptionKey"),
            "Authorization": f"Bearer {self.__token}",
        }

    def __refresh_auth_if_needed(self) -> None:
        """
        Refresh authentication headers if token is missing.

        Private method that checks if valid authentication headers exist and
        regenerates them if necessary.
        """
        if "error" in self.__request_headers or not self.__token:
            self.__request_headers = self.__setup_request_headers()

    def __send_request(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Send an HTTP request to the gateway API.

        Private method that handles the actual HTTP request and response processing.

        Args:
            url: The endpoint URL to send the request to
            payload: The JSON payload to include in the request

        Returns:
            Dict[str, Any]: The JSON response from the API

        Raises:
            requests.exceptions.RequestException: If the request fails
        """
        logger.debug(f"Sending request to {url} with payload: {payload}")
        response: requests.Response = requests.post(
            url, json=payload, headers=self.__request_headers
        )
        response.raise_for_status()
        return response.json()

    def import_dataset(
        self,
        dataset_name: str,
        dataset_description: str,
        schema_id: UUID | None,
        tags: dict[str, str],
        folder_id: UUID,
    ) -> dict[str, Any]:
        """
        Imports a dataset into the DataWorkbench.

        Creates a new dataset entry in the data catalog with the provided metadata.

        Args:
            dataset_name: Name of the dataset to be imported
            dataset_description: Description of the dataset's purpose and contents
            schema_id: UUID identifier for the dataset's schema
            tags: Metadata tags as key-value pairs for categorization and discovery
            folder_id: UUID identifier for the folder where the dataset will be stored

        Returns:
            Dict[str, Any]: JSON response from the API or an error message dictionary

        Example:
            >>> gateway = Gateway()
            >>> response = gateway.import_dataset(
            ...     "Sales Data 2023",
            ...     "Monthly sales figures for all regions",
            ...     UUID("550e8400-e29b-41d4-a716-446655440000"),
            ...     {"department": "sales", "region": "global"},
            ...     UUID("123e4567-e89b-12d3-a456-426614174000")
            ... )
        """
        self.__refresh_auth_if_needed()

        url: str = f"{self.__gateway_url}/workspaces/{self.__workspace_id}/ingest/datasets/import"
        payload: dict[str, Any] = {
            "datasetFolderId": str(folder_id),
            "datasetName": dataset_name,
            "datasetDescription": dataset_description,
            "schemaId": str(schema_id) if schema_id else schema_id,
            "tags": tags,
        }

        logger.info(f"Sending request to import dataset: {dataset_name}")

        try:
            response = self.__send_request(url, payload)
            logger.info(
                f"Successfully imported dataset {dataset_name} to Data Workbench"
            )
            return response
        except requests.exceptions.RequestException as e:
            trace_id = (
                _get_trace_id_from_response(e.response)
                if e.response is not None
                else None
            )
            logger.error(f"Error creating data catalog entry: {e}")
            return {
                "error": "Failed to create data catalog entry.",
                "correlation_id": trace_id,
            }
