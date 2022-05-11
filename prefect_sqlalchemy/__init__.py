from . import _version
from .credentials import DatabaseCredentials, AsyncDriver  # noqa

__version__ = _version.get_versions()["version"]
