import requests
from typing import Optional

from dataworkbench.utils import get_secret
from dataworkbench.log import setup_logger

# Configure logging
logger = setup_logger(__name__)


class TokenManager:
    """
    Handles retrieval and management of authentication tokens.
    
    This class provides functionality to obtain access tokens for API authentication
    using the client credentials flow.
    
    Public API:
    - get_token: Retrieves an access token for API authentication
    
    All other methods are for internal use only and may change without notice.
    """
    
    @staticmethod
    def get_token() -> Optional[str]:
        """
        Retrieve an access token for API authentication.
        
        Obtains a new access token using client credentials from the authentication server.
        
        Returns:
            Optional[str]: The access token if successful, None if token retrieval fails
            
        Example:
            >>> token = TokenManager.get_token()
            >>> if token:
            ...     print("Token retrieved successfully")
            ... else:
            ...     print("Failed to retrieve token")
        """
        try:
            return TokenManager.__request_token()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve access token: {e}")
            return None
            
    @staticmethod
    def __request_token() -> str:
        """
        Make the actual HTTP request to obtain an access token.
        
        This is an internal method not intended for direct use.
        
        Returns:
            str: The access token
            
        Raises:
            requests.exceptions.RequestException: If the token request fails
        """
        response = requests.post(
            get_secret("ApimTokenUrl"), 
            data={
                "grant_type": "client_credentials",
                "client_id": get_secret("ApimClientId"),
                "client_secret": get_secret("ApimClientSecret"),
                "scope": get_secret("ApimScope")
            }
        )
        response.raise_for_status()
        return response.json().get("access_token")