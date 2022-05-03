from . import _version
from .credentials import SQLAlchemyCredentials  # noqa

__version__ = _version.get_versions()["version"]
