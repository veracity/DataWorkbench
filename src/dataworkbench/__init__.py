from dataworkbench.datacatalogue import DataCatalogue

import importlib.metadata

try:
    __version__ = importlib.metadata.version("dataworkbench")
except importlib.metadata.PackageNotFoundError:
    print("Package not installed")

__all__ = [
    "DataCatalogue"
]
