from dataworkbench.utils import get_secret
from dataworkbench.gateway import Gateway
from dataworkbench.log import setup_logger

# Configure logging
logger = setup_logger(__name__)

class MMS:
    def __init__(self):
        self.gateway = Gateway(get_secret("GatewayBaseUrl"), get_secret("DwbWorkspaceId"))
        self.workspace_id = get_secret("DwbWorkspaceId")
        self.storage_account_name = get_secret("ByodStorageName")
