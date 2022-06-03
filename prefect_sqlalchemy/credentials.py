"""Credential classes used to perform authenticated interactions with SQLAlchemy"""

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.ext.asyncio.engine import AsyncConnection


class AsyncDriver(Enum):
    """
    Known dialects with their corresponding async drivers.
    """

    POSTGRESQL_ASYNCPG = "postgresql+asyncpg"

    SQLITE_AIOSQLITE = "sqlite+aiosqlite"

    MYSQL_ASYNCMY = "mysql+asyncmy"
    MYSQL_AIOMYSQL = "mysql+aiomysql"


class SyncDriver(Enum):
    """
    Known dialects with their corresponding sync drivers.
    """

    POSTGRESQL_PSYCOPG2 = "postgresql+psycopg2"
    POSTGRESQL_PG8000 = "postgresql+pg8000"
    POSTGRESQL_PSYCOPG2CFFI = "postgresql+psycopg2cffi"
    POSTGRESQL_PYPOSTGRESQL = "postgresql+pypostgresql"
    POSTGRESQL_PYGRESQL = "postgresql+pygresql"

    MYSQL_MYSQLDB = "mysql+mysqldb"
    MYSQL_PYMYSQL = "mysql+pymysql"
    MYSQL_MYSQLCONNECTOR = "mysql+mysqlconnector"
    MYSQL_CYMYSQL = "mysql+cymysql"
    MYSQL_OURSQL = "mysql+oursql"
    MYSQL_PYODBC = "mysql+pyodbc"

    SQLITE_PYSQLITE = "sqlite+pysqlite"
    SQLITE_PYSQLCIPHER = "sqlite+pysqlcipher"

    ORACLE_CX_ORACLE = "oracle+cx_oracle"

    MSSQL_PYODBC = "mssql+pyodbc"
    MSSQL_MXODBC = "mssql+mxodbc"
    MSSQL_PYMSSQL = "mssql+pymssql"


@dataclass
class DatabaseCredentials:
    """
    Dataclass used to manage authentication with SQLAlchemy.

    Args:
        driver: The driver name, e.g. "postgresql+asyncpg"
        database: The name of the database to use.
        username: The user name used to authenticate.
        password: The password used to authenticate.
        host: The host address of the database.
        port: The port to connect to the database.
        query: A dictionary of string keys to string values to be passed to
            the dialect and/or the DBAPI upon connect. To specify non-string
            parameters to a Python DBAPI directly, use connect_args.
        url: Manually create and provide a URL to create the engine,
            this is useful for external dialects, e.g. Snowflake, because some
            of the params, such as "warehouse", is not directly supported in
            the vanilla `sqlalchemy.engine.URL.create` method; do not provide
            this alongside with other URL params as it will raise a `ValueError`.
        connect_args: The options which will be passed directly to the
            DBAPI's connect() method as additional keyword arguments.
    """

    driver: Optional[Union[AsyncDriver, SyncDriver, str]] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    host: Optional[str] = None
    port: Optional[str] = None
    query: Optional[Dict[str, str]] = None
    url: Optional[Union[URL, str]] = None
    connect_args: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        """
        Initializes the engine.
        """
        if isinstance(self.driver, AsyncDriver):
            drivername = self.driver.value
            self._async_supported = True
        elif isinstance(self.driver, SyncDriver):
            drivername = self.driver.value
            self._async_supported = False
        else:
            drivername = self.driver
            self._async_supported = drivername in AsyncDriver._value2member_map_

        url_params = dict(
            drivername=drivername,
            username=self.username,
            password=self.password,
            database=self.database,
            host=self.host,
            port=self.port,
            query=self.query,
        )
        if not self.url:
            required_url_keys = ("drivername", "username", "database")
            if not all(url_params[key] for key in required_url_keys):
                raise ValueError(
                    f"If the `url` is not provided, "
                    f"all of these URL params are required: "
                    f"{required_url_keys}"
                )
            self.url = URL.create(**url_params)  # from params
        else:
            if any(val for val in url_params.values()):
                raise ValueError(
                    f"The `url` should not be provided "
                    f"alongside any of these URL params: "
                    f"{url_params.keys()}"
                )
            if not isinstance(self.url, URL):
                self.url = make_url(self.url)  # from string

    def get_engine(self) -> Union["Connection", "AsyncConnection"]:
        """
        Returns an authenticated engine that can be
        used to query from databases.

        Returns:
            The authenticated SQLAlchemy Connection / AsyncConnection.

        Examples:
            Create an asynchronous engine to PostgreSQL using URL params.
            ```python
            from prefect import flow
            from prefect_sqlalchemy import DatabaseCredentials, AsyncDriver

            @flow
            def sqlalchemy_credentials_flow():
                sqlalchemy_credentials = DatabaseCredentials(
                    driver=AsyncDriver.POSTGRESQL_ASYNCPG,
                    username="prefect",
                    password="prefect_password",
                    database="postgres"
                )
                print(sqlalchemy_credentials.get_engine())

            sqlalchemy_credentials_flow()
            ```

            Create a synchronous engine to Snowflake using the `url` kwarg.
            ```python
            from prefect import flow
            from prefect_sqlalchemy import DatabaseCredentials, AsyncDriver

            @flow
            def sqlalchemy_credentials_flow():
                url = (
                    "snowflake://<user_login_name>:<password>"
                    "@<account_identifier>/<database_name>"
                    "?warehouse=<warehouse_name>"
                )
                sqlalchemy_credentials = DatabaseCredentials(url=url)
                print(sqlalchemy_credentials.get_engine())

            sqlalchemy_credentials_flow()
            ```
        """
        engine_kwargs = dict(
            url=self.url, connect_args=self.connect_args or {}, poolclass=NullPool
        )
        if self._async_supported:
            engine = create_async_engine(**engine_kwargs)
        else:
            engine = create_engine(**engine_kwargs)
        return engine
