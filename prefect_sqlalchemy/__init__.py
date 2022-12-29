from . import _version
from .credentials import (  # noqa
    DatabaseCredentials,
    ConnectionComponents,
    AsyncDriver,
    SyncDriver,
)

__version__ = _version.get_versions()["version"]
