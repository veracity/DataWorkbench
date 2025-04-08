import os
import pytest
from unittest.mock import patch

# Arrange
@pytest.fixture(scope="function", autouse=True)
def setenvvar(monkeypatch):
    with patch.dict(os.environ, clear=True):
        envvars = {
            "GatewayBaseUrl": "http://mock-gateway-base-url.com",
            "DwbWorkspaceId": "mock-dwb-workspace-id",
            "ByodStorageName": "mock-byod-storage-name",
            "OcpApimSubscriptionKey": "mock-ocp-apim-subscription-key",
            "ApimTokenUrl": "http://mock-apim-token-url.com",
            "ApimClientId": "mock-apim-client-id",
            "ApimClientSecret": "mock-apim-client-secret",
            "ApimScope": "mock-apim-scope",
            "StorageBaseUrl": "mock-storage-url"
        }

        for k, v in envvars.items():
            monkeypatch.setenv(k, v)
        yield