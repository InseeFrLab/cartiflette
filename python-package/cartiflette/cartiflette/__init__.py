from importlib.metadata import version

from .config import _config
from .client import carti_download, get_catalog

__version__ = version(__package__)

__all__ = ["carti_download", "get_catalog"]
