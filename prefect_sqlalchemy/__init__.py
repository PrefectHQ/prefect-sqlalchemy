from . import _version
from .credentials import DatabaseCredentials, AsyncDriver, SyncDriver  # noqa

__version__ = _version.get_versions()["version"]
