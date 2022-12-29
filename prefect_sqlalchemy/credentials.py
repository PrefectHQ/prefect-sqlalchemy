"""Credential classes used to perform authenticated interactions with SQLAlchemy"""

import warnings
from contextlib import AsyncExitStack, ExitStack, asynccontextmanager
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from prefect.blocks.abstract import CredentialsBlock, DatabaseBlock
from prefect.blocks.core import Block
from prefect.utilities.asyncutils import sync_compatible
from prefect.utilities.hashing import hash_objects
from pydantic import AnyUrl, BaseModel, Field, SecretStr
from sqlalchemy.engine import Connection, Engine, create_engine
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text


class AsyncDriver(Enum):
    """
    Known dialects with their corresponding async drivers.

    Attributes:
        POSTGRESQL_ASYNCPG (Enum): [postgresql+asyncpg](https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.asyncpg)

        SQLITE_AIOSQLITE (Enum): [sqlite+aiosqlite](https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#module-sqlalchemy.dialects.sqlite.aiosqlite)

        MYSQL_ASYNCMY (Enum): [mysql+asyncmy](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#module-sqlalchemy.dialects.mysql.asyncmy)
        MYSQL_AIOMYSQL (Enum): [mysql+aiomysql](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#module-sqlalchemy.dialects.mysql.aiomysql)
    """  # noqa

    POSTGRESQL_ASYNCPG = "postgresql+asyncpg"

    SQLITE_AIOSQLITE = "sqlite+aiosqlite"

    MYSQL_ASYNCMY = "mysql+asyncmy"
    MYSQL_AIOMYSQL = "mysql+aiomysql"


class SyncDriver(Enum):
    """
    Known dialects with their corresponding sync drivers.

    Attributes:
        POSTGRESQL_PSYCOPG2 (Enum): [postgresql+psycopg2](https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg2)
        POSTGRESQL_PG8000 (Enum): [postgresql+pg8000](https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.pg8000)
        POSTGRESQL_PSYCOPG2CFFI (Enum): [postgresql+psycopg2cffi](https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg2cffi)
        POSTGRESQL_PYPOSTGRESQL (Enum): [postgresql+pypostgresql](https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.pypostgresql)
        POSTGRESQL_PYGRESQL (Enum): [postgresql+pygresql](https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.pygresql)

        MYSQL_MYSQLDB (Enum): [mysql+mysqldb](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#module-sqlalchemy.dialects.mysql.mysqldb)
        MYSQL_PYMYSQL (Enum): [mysql+pymysql](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#module-sqlalchemy.dialects.mysql.pymysql)
        MYSQL_MYSQLCONNECTOR (Enum): [mysql+mysqlconnector](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#module-sqlalchemy.dialects.mysql.mysqlconnector)
        MYSQL_CYMYSQL (Enum): [mysql+cymysql](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#module-sqlalchemy.dialects.mysql.cymysql)
        MYSQL_OURSQL (Enum): [mysql+oursql](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#module-sqlalchemy.dialects.mysql.oursql)
        MYSQL_PYODBC (Enum): [mysql+pyodbc](https://docs.sqlalchemy.org/en/14/dialects/mysql.html#module-sqlalchemy.dialects.mysql.pyodbc)

        SQLITE_PYSQLITE (Enum): [sqlite+pysqlite](https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#module-sqlalchemy.dialects.sqlite.pysqlite)
        SQLITE_PYSQLCIPHER (Enum): [sqlite+pysqlcipher](https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#module-sqlalchemy.dialects.sqlite.pysqlcipher)

        ORACLE_CX_ORACLE (Enum): [oracle+cx_oracle](https://docs.sqlalchemy.org/en/14/dialects/oracle.html#module-sqlalchemy.dialects.oracle.cx_oracle)

        MSSQL_PYODBC (Enum): [mssql+pyodbc](https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pyodbc)
        MSSQL_MXODBC (Enum): [mssql+mxodbc](https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.mxodbc)
        MSSQL_PYMSSQL (Enum): [mssql+pymssql](https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pymssql)
    """  # noqa

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


class DatabaseCredentials(Block):
    """
    Block used to manage authentication with a database.

    Attributes:
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

    Example:
        Load stored database credentials:
        ```python
        from prefect_sqlalchemy import DatabaseCredentials
        database_block = DatabaseCredentials.load("BLOCK_NAME")
        ```
    """

    _block_type_name = "Database Credentials"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/3xLant5G70S4vJpmdWCYmr/8fdb19f15b97c3a07c3af3efde4d28fb/download.svg.png?h=250"  # noqa

    driver: Optional[Union[AsyncDriver, SyncDriver, str]] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    database: Optional[str] = None
    host: Optional[str] = None
    port: Optional[str] = None
    query: Optional[Dict[str, str]] = None
    url: Optional[AnyUrl] = None
    connect_args: Optional[Dict[str, Any]] = None

    def block_initialization(self):
        """
        Initializes the engine.
        """
        warnings.warn(
            "DatabaseCredentials is now deprecated and will be removed March 2023; "
            "please use SqlAlchemyConnector instead.",
            DeprecationWarning,
        )
        if isinstance(self.driver, AsyncDriver):
            drivername = self.driver.value
            self._driver_is_async = True
        elif isinstance(self.driver, SyncDriver):
            drivername = self.driver.value
            self._driver_is_async = False
        else:
            drivername = self.driver
            self._driver_is_async = drivername in AsyncDriver._value2member_map_

        url_params = dict(
            drivername=drivername,
            username=self.username,
            password=self.password.get_secret_value() if self.password else None,
            database=self.database,
            host=self.host,
            port=self.port,
            query=self.query,
        )
        if not self.url:
            required_url_keys = ("drivername", "database")
            if not all(url_params[key] for key in required_url_keys):
                required_url_keys = ("driver", "database")
                raise ValueError(
                    f"If the `url` is not provided, "
                    f"all of these URL params are required: "
                    f"{required_url_keys}"
                )
            self.rendered_url = URL.create(
                **{
                    url_key: url_param
                    for url_key, url_param in url_params.items()
                    if url_param is not None
                }
            )  # from params
        else:
            if any(val for val in url_params.values()):
                raise ValueError(
                    f"The `url` should not be provided "
                    f"alongside any of these URL params: "
                    f"{url_params.keys()}"
                )
            self.rendered_url = make_url(str(self.url))

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
            url=self.rendered_url,
            connect_args=self.connect_args or {},
            poolclass=NullPool,
        )
        if self._driver_is_async:
            engine = create_async_engine(**engine_kwargs)
        else:
            engine = create_engine(**engine_kwargs)
        return engine

    class Config:
        """Configuration of pydantic."""

        # Support serialization of the 'URL' type
        arbitrary_types_allowed = True
        json_encoders = {URL: lambda u: u.render_as_string()}

    def dict(self, *args, **kwargs) -> Dict:
        """
        Convert to a dictionary.
        """
        # Support serialization of the 'URL' type
        d = super().dict(*args, **kwargs)
        d["rendered_url"] = SecretStr(
            self.rendered_url.render_as_string(hide_password=False)
        )
        return d


class SqlAlchemyUrl(BaseModel):
    driver: Union[AsyncDriver, SyncDriver, str] = Field(
        default=..., description="The driver name to use."
    )
    database: str = Field(default=..., description="The name of the database to use.")
    username: Optional[str] = Field(
        default=None, description="The user name used to authenticate."
    )
    password: Optional[SecretStr] = Field(
        default=None, description="The password used to authenticate."
    )
    host: Optional[str] = Field(
        default=None, description="The host address of the database."
    )
    port: Optional[str] = Field(
        default=None, description="The port to connect to the database."
    )
    query: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "A dictionary of string keys to string values to be passed to the dialect "
            "and/or the DBAPI upon connect. To specify non-string parameters to a "
            "Python DBAPI directly, use connect_args."
        ),
    )


class SqlAlchemyConnector(CredentialsBlock, DatabaseBlock):
    """
    Block used to manage authentication with a database.

    Upon instantiating, an engine is created and maintained for the life of
    the object until the close method is called.

    It is recommended to use this block as a context manager, which will automatically
    close the engine and its connections when the context is exited.

    It is also recommended that this block is loaded and consumed within a single task
    or flow because if the block is passed across separate tasks and flows,
    the state of the block's connection and cursor could be lost.

    Attributes:
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
        fetch_size: The number of rows to fetch at a time.

    Example:
        Load stored database credentials and use in context manager:
        ```python
        from prefect_sqlalchemy import SqlAlchemyConnector

        database_block = SqlAlchemyConnector.load("BLOCK_NAME")
        with database_block:
            ...
        ```

        Create table named customers and insert values; then fetch the first 10 rows.
        ```python
        from prefect_sqlalchemy import SqlAlchemyConnector, SyncDriver

        with SqlAlchemyConnector(
            driver=SyncDriver.SQLITE_PYSQLITE,
            database="prefect.db"
        ) as database:
            database.execute(
                "CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);",
            )
            for i in range(1, 42):
                database.execute(
                    "INSERT INTO customers (name, address) VALUES (:name, :address);",
                    parameters={"name": "Marvin", "address": f"Highway {i}"},
                )
            results = database.fetch_many(
                "SELECT * FROM customers WHERE name = :name;",
                parameters={"name": "Marvin"},
                size=10
            )
        print(results)
        ```
    """

    _block_type_name = "SQLAlchemy Connector"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/37TOcxeP9kfXffpKVRAHiJ/0f359112e79d0bd3dfe38c73c4fc6363/sqlalchemy.png?h=250"  # noqa

    url: Union[SqlAlchemyUrl, AnyUrl] = Field(
        default=...,
        description=(
            "SQLAlchemy URL to create the engine; either create from components "
            "or create from a string."
        ),
    )
    connect_args: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "The options which will be passed directly to the DBAPI's connect() "
            "method as additional keyword arguments."
        ),
    )
    fetch_size: int = Field(
        default=1, description="The number of rows to fetch at a time."
    )

    _engine: Optional[Union[AsyncEngine, Engine]] = None
    _exit_stack: Union[ExitStack, AsyncExitStack] = None
    _unique_results: Dict[str, CursorResult] = None

    def block_initialization(self):
        """
        Initializes the engine.
        """
        super().block_initialization()

        if isinstance(self.url, SqlAlchemyUrl):
            driver = self.url.driver
            drivername = driver.value if isinstance(driver, Enum) else driver
            if self.url.password:
                password = self.url.password.get_secret_value()
            else:
                password = None
            url_params = dict(
                drivername=drivername,
                username=self.url.username,
                password=password,
                database=self.url.database,
                host=self.url.host,
                port=self.url.port,
                query=self.url.query,
            )
            self._rendered_url = URL.create(
                **{
                    url_key: url_param
                    for url_key, url_param in url_params.items()
                    if url_param is not None
                }
            )
        else:
            # make rendered url from string
            self._rendered_url = make_url(str(self.url))
            drivername = self._rendered_url.drivername

        try:
            AsyncDriver(drivername)
            self._driver_is_async = True
        except ValueError:
            self._driver_is_async = False

        if self._engine is None:
            self._start_engine()

        if self._exit_stack is None:
            self._start_exit_stack()

        if self._unique_results is None:
            self._unique_results = {}

    def _start_engine(self):
        """
        Starts SQLAlchemy database engine.
        """
        self._engine = self.get_engine()

    def _start_exit_stack(self):
        """
        Starts an AsyncExitStack or ExitStack depending on whether driver is async.
        """
        self._exit_stack = AsyncExitStack() if self._driver_is_async else ExitStack()

    def get_engine(self, **create_engine_kwargs) -> Union[Engine, AsyncEngine]:
        """
        Returns an authenticated engine that can be
        used to query from databases.

        If an existing engine exists, return that one.

        Returns:
            The authenticated SQLAlchemy Engine / AsyncEngine.

        Examples:
            Create an asynchronous engine to PostgreSQL using URL params.
            ```python
            from prefect import flow
            from prefect_sqlalchemy import SqlAlchemyConnector, AsyncDriver

            @flow
            def sqlalchemy_credentials_flow():
                sqlalchemy_credentials = SqlAlchemyConnector(
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
            from prefect_sqlalchemy import SqlAlchemyConnector, AsyncDriver

            @flow
            def sqlalchemy_credentials_flow():
                url = (
                    "snowflake://<user_login_name>:<password>"
                    "@<account_identifier>/<database_name>"
                    "?warehouse=<warehouse_name>"
                )
                sqlalchemy_credentials = SqlAlchemyConnector(url=url)
                print(sqlalchemy_credentials.get_engine())

            sqlalchemy_credentials_flow()
            ```
        """
        if self._engine is not None:
            self.logger.debug("Reusing existing engine.")
            return self._engine

        engine_kwargs = dict(
            url=self._rendered_url,
            connect_args=self.connect_args or {},
            **create_engine_kwargs,
        )
        if self._driver_is_async:
            # no need to await here
            engine = create_async_engine(**engine_kwargs)
        else:
            engine = create_engine(**engine_kwargs)
        self.logger.info("Created a new engine.")
        return engine

    def get_connection(
        self, begin: bool = True, **connect_kwargs
    ) -> Union[Connection, AsyncConnection]:
        """
        Returns a connection that can be used to query from databases.

        Args:
            begin: Whether to begin a transaction on the connection; if True, if
                any operations fail, the entire transaction will be rolled back.
            **connect_kwargs: Additional keyword arguments to pass to either
                `engine.begin` or engine.connect`.

        Returns:
            The SQLAlchemy Connection / AsyncConnection.

        Examples:
            Create an synchronous connection as a context-managed transaction.
            ```python
            from prefect_sqlalchemy import SqlAlchemyConnector

            sqlalchemy_connector = SqlAlchemyConnector.load("BLOCK_NAME")
            with sqlalchemy_connector.get_connection(begin=False) as connection:
                connection.execute("SELECT * FROM table LIMIT 1;")
            ```

            Create an asynchronous connection as a context-managed transacation.
            ```python
            import asyncio
            from prefect_sqlalchemy import SqlAlchemyConnector

            sqlalchemy_connector = SqlAlchemyConnector.load("BLOCK_NAME")
            async with sqlalchemy_connector.get_connection(begin=False) as connection:
                asyncio.run(connection.execute("SELECT * FROM table LIMIT 1;"))
            ```
        """  # noqa: E501
        engine = self.get_engine()
        if begin:
            connection = engine.begin(**connect_kwargs)
        else:
            connection = engine.connect(**connect_kwargs)
        self.logger.info("Created a new connection.")
        return connection

    def get_client(
        self, client_type: Literal["engine", "connection"], **get_client_kwargs
    ) -> Union[Engine, AsyncEngine, Connection, AsyncConnection]:
        """
        Returns either an engine or connection that can be used to query from databases.

        Args:
            client_type: Select from either 'engine' or 'connection'.
            **get_client_kwargs: Additional keyword arguments to pass to
                either `get_engine` or `get_connection`.

        Returns:
            The authenticated SQLAlchemy engine or connection.

        Examples:
            Create an engine.
            ```
            from prefect_sqlalchemy import SqlalchemyConnector

            sqlalchemy_connector = SqlAlchemyConnector.load("BLOCK_NAME")
            engine = sqlalchemy_connector.get_client(client_type="engine")
            ```

            Create a context managed connection.
            ```
            from prefect_sqlalchemy import SqlalchemyConnector

            sqlalchemy_connector = SqlAlchemyConnector.load("BLOCK_NAME")
            with sqlalchemy_connector.get_client(client_type="connection") as conn:
                ...
            ```
        """  # noqa: E501
        if client_type == "engine":
            client = self.get_engine(**get_client_kwargs)
        elif client_type == "connection":
            client = self.get_connection(**get_client_kwargs)
        else:
            raise ValueError(
                f"{client_type!r} is not supported; choose from engine or connection."
            )
        return client

    async def _async_sync_execute(
        self,
        connection: Union[Connection, AsyncConnection],
        *execute_args: Tuple[Any],
        **execute_kwargs: Dict[str, Any],
    ) -> CursorResult:
        """
        Execute the statement asynchronously or synchronously.
        """
        # can't use run_sync_in_worker_thread:
        # ProgrammingError: (sqlite3.ProgrammingError) SQLite objects created in a
        # thread can only be used in that same thread.
        result_set = connection.execute(*execute_args, **execute_kwargs)

        if self._driver_is_async:
            result_set = await result_set
            await connection.commit()  # very important
        return result_set

    @asynccontextmanager
    async def _manage_connection(self, **get_connection_kwargs: Dict[str, Any]):
        if self._driver_is_async:
            async with self.get_connection(**get_connection_kwargs) as connection:
                yield connection
        else:
            with self.get_connection(**get_connection_kwargs) as connection:
                yield connection

    async def _get_result_set(
        self, *execute_args: Tuple[Any], **execute_kwargs: Dict[str, Any]
    ) -> CursorResult:
        """
        Returns a new or existing result set based on whether the inputs
        are unique.

        Args:
            *execute_args: Args to pass to execute.
            **execute_kwargs: Keyword args to pass to execute.

        Returns:
            The result set from the operation.
        """  # noqa: E501
        input_hash = hash_objects(*execute_args, **execute_kwargs)
        assert input_hash is not None, (
            "We were not able to hash your inputs, "
            "which resulted in an unexpected data return; "
            "please open an issue with a reproducible example."
        )

        if input_hash not in self._unique_results.keys():
            if self._driver_is_async:
                connection = await self._exit_stack.enter_async_context(
                    self.get_connection()
                )
            else:
                connection = self._exit_stack.enter_context(self.get_connection())
            result_set = await self._async_sync_execute(
                connection, *execute_args, **execute_kwargs
            )
            # implicitly store the connection by storing the result set
            # which points to its parent connection
            self._unique_results[input_hash] = result_set
        else:
            result_set = self._unique_results[input_hash]
        return result_set

    def _reset_cursor_results(self) -> None:
        """
        Closes all the existing cursor results.
        """
        input_hashes = tuple(self._unique_results.keys())
        for input_hash in input_hashes:
            try:
                cursor_result = self._unique_results.pop(input_hash)
                cursor_result.close()
            except Exception as exc:
                self.logger.warning(
                    f"Failed to close connection for input hash {input_hash!r}: {exc}"
                )

    def reset_connections(self) -> None:
        """
        Tries to close all opened connections and their results.

        Examples:
            Resets connections so fetch_* methods return new results.
            ```python
            from prefect_sqlalchemy import SqlAlchemyConnector

            with SqlAlchemyConnector.load("MY_BLOCK") as database:
                results = database.fetch_one("SELECT * FROM customers")
                database.reset_connections()
                results = database.fetch_one("SELECT * FROM customers")
            ```
        """
        if self._driver_is_async:
            raise RuntimeError(
                f"{self._rendered_url.drivername} has no synchronous connections. "
                f"Please use the `reset_async_connections` method instead."
            )
        self._reset_cursor_results()
        self._exit_stack.close()
        self.logger.info("Reset opened connections and their results.")

    async def reset_async_connections(self) -> None:
        """
        Tries to close all opened connections and their results.

        Examples:
            Resets connections so fetch_* methods return new results.
            ```python
            import asyncio
            from prefect_sqlalchemy import SqlAlchemyConnector

            async def example_run():
                async with SqlAlchemyConnector.load("MY_BLOCK") as database:
                    results = await database.fetch_one("SELECT * FROM customers")
                    await database.reset_async_connections()
                    results = await database.fetch_one("SELECT * FROM customers")

            asyncio.run(example_run())
            ```
        """
        if not self._driver_is_async:
            raise RuntimeError(
                f"{self._rendered_url.drivername} has no asynchronous connections. "
                f"Please use the `reset_connections` method instead."
            )
        self._reset_cursor_results()
        await self._exit_stack.aclose()
        self.logger.info("Reset opened connections and their results.")

    @sync_compatible
    async def fetch_one(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        **execution_options: Dict[str, Any],
    ) -> Tuple[Any]:
        """
        Fetch a single result from the database.

        Repeated calls using the same inputs to *any* of the fetch methods of this
        block will skip executing the operation again, and instead,
        return the next set of results from the previous execution,
        until the reset_cursors method is called.

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            **execution_options: Options to pass to `Connection.execution_options`.

        Returns:
            A list of tuples containing the data returned by the database,
                where each row is a tuple and each column is a value in the tuple.

        Examples:
            Create a table, insert three rows into it, and fetch a row repeatedly.
            ```python
            from prefect_sqlalchemy import SqlAlchemyConnector

            with SqlAlchemyConnector.load("MY_BLOCK") as database:
                database.execute("CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);")
                database.execute_many(
                    "INSERT INTO customers (name, address) VALUES (:name, :address);",
                    seq_of_parameters=[
                        {"name": "Ford", "address": "Highway 42"},
                        {"name": "Unknown", "address": "Space"},
                        {"name": "Me", "address": "Myway 88"},
                    ],
                )
                results = True
                while results:
                    results = database.fetch_one("SELECT * FROM customers")
                    print(results)
            ```
        """  # noqa
        result_set = await self._get_result_set(
            text(operation), parameters, execution_options=execution_options
        )
        self.logger.debug("Preparing to fetch one row.")
        row = result_set.fetchone()
        return row

    @sync_compatible
    async def fetch_many(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        size: Optional[int] = None,
        **execution_options: Dict[str, Any],
    ) -> List[Tuple[Any]]:
        """
        Fetch a limited number of results from the database.

        Repeated calls using the same inputs to *any* of the fetch methods of this
        block will skip executing the operation again, and instead,
        return the next set of results from the previous execution,
        until the reset_cursors method is called.

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            size: The number of results to return; if None or 0, uses the value of
                `fetch_size` configured on the block.
            **execution_options: Options to pass to `Connection.execution_options`.

        Returns:
            A list of tuples containing the data returned by the database,
                where each row is a tuple and each column is a value in the tuple.

        Examples:
            Create a table, insert three rows into it, and fetch two rows repeatedly.
            ```python
            from prefect_sqlalchemy import SqlAlchemyConnector

            with SqlAlchemyConnector.load("MY_BLOCK") as database:
                database.execute("CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);")
                database.execute_many(
                    "INSERT INTO customers (name, address) VALUES (:name, :address);",
                    seq_of_parameters=[
                        {"name": "Ford", "address": "Highway 42"},
                        {"name": "Unknown", "address": "Space"},
                        {"name": "Me", "address": "Myway 88"},
                    ],
                )
                results = database.fetch_many("SELECT * FROM customers", size=2)
                print(results)
                results = database.fetch_many("SELECT * FROM customers", size=2)
                print(results)
            ```
        """  # noqa
        result_set = await self._get_result_set(
            text(operation), parameters, execution_options=execution_options
        )
        size = size or self.fetch_size
        self.logger.debug(f"Preparing to fetch {size} rows.")
        rows = result_set.fetchmany(size=size)
        return rows

    @sync_compatible
    async def fetch_all(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        **execution_options: Dict[str, Any],
    ) -> List[Tuple[Any]]:
        """
        Fetch all results from the database.

        Repeated calls using the same inputs to *any* of the fetch methods of this
        block will skip executing the operation again, and instead,
        return the next set of results from the previous execution,
        until the reset_cursors method is called.

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            **execution_options: Options to pass to `Connection.execution_options`.

        Returns:
            A list of tuples containing the data returned by the database,
                where each row is a tuple and each column is a value in the tuple.

        Examples:
            Create a table, insert three rows into it, and fetch all where name is 'Me'.
            ```python
            from prefect_sqlalchemy import SqlAlchemyConnector

            with SqlAlchemyConnector.load("MY_BLOCK") as database:
                database.execute("CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);")
                database.execute_many(
                    "INSERT INTO customers (name, address) VALUES (:name, :address);",
                    seq_of_parameters=[
                        {"name": "Ford", "address": "Highway 42"},
                        {"name": "Unknown", "address": "Space"},
                        {"name": "Me", "address": "Myway 88"},
                    ],
                )
                results = database.fetch_all("SELECT * FROM customers WHERE name = :name", parameters={"name": "Me"})
            ```
        """  # noqa
        result_set = await self._get_result_set(
            text(operation), parameters, execution_options=execution_options
        )
        self.logger.debug("Preparing to fetch all rows.")
        rows = result_set.fetchall()
        return rows

    @sync_compatible
    async def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        **execution_options: Dict[str, Any],
    ) -> None:
        """
        Executes an operation on the database. This method is intended to be used
        for operations that do not return data, such as INSERT, UPDATE, or DELETE.

        Unlike the fetch methods, this method will always execute the operation
        upon calling.

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            **execution_options: Options to pass to `Connection.execution_options`.

        Examples:
            Create a table and insert one row into it.
            ```python
            from prefect_sqlalchemy import SqlAlchemyConnector

            with SqlAlchemyConnector.load("MY_BLOCK") as database:
                database.execute("CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);")
                database.execute(
                    "INSERT INTO customers (name, address) VALUES (:name, :address);",
                    parameters={"name": "Marvin", "address": "Highway 42"},
                )
            ```
        """  # noqa
        async with self._manage_connection(begin=False) as connection:
            await self._async_sync_execute(
                connection,
                text(operation),
                parameters,
                execution_options=execution_options,
            )
        self.logger.info(f"Executed the operation, {operation!r}")

    @sync_compatible
    async def execute_many(
        self,
        operation: str,
        seq_of_parameters: List[Dict[str, Any]],
        **execution_options: Dict[str, Any],
    ) -> None:
        """
        Executes many operations on the database. This method is intended to be used
        for operations that do not return data, such as INSERT, UPDATE, or DELETE.

        Unlike the fetch methods, this method will always execute the operation
        upon calling.

        Args:
            operation: The SQL query or other operation to be executed.
            seq_of_parameters: The sequence of parameters for the operation.
            **execution_options: Options to pass to `Connection.execution_options`.

        Examples:
            Create a table and insert two rows into it.
            ```python
            from prefect_sqlalchemy import SqlAlchemyConnector

            with SqlAlchemyConnector.load("MY_BLOCK") as database:
                database.execute("CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);")
                database.execute_many(
                    "INSERT INTO customers (name, address) VALUES (:name, :address);",
                    seq_of_parameters=[
                        {"name": "Ford", "address": "Highway 42"},
                        {"name": "Unknown", "address": "Space"},
                        {"name": "Me", "address": "Myway 88"},
                    ],
                )
            ```
        """  # noqa
        async with self._manage_connection(begin=False) as connection:
            await self._async_sync_execute(
                connection,
                text(operation),
                seq_of_parameters,
                execution_options=execution_options,
            )
        self.logger.info(
            f"Executed {len(seq_of_parameters)} operations based off {operation!r}."
        )

    async def __aenter__(self):
        """
        Start an asynchronous database engine upon entry.
        """
        if not self._driver_is_async:
            raise RuntimeError(
                f"{self._rendered_url.drivername} cannot be run asynchronously. "
                f"Please use the `with` syntax."
            )
        return self

    async def __aexit__(self, *args):
        """
        Dispose the asynchronous database engine upon exit.
        """
        await self.aclose()

    async def aclose(self):
        """
        Closes async connections and its cursors.
        """
        if not self._driver_is_async:
            raise RuntimeError(
                f"{self._rendered_url.drivername} is not asynchronous. "
                f"Please use the `close` method instead."
            )
        try:
            await self.reset_async_connections()
        finally:
            if self._engine is not None:
                await self._engine.dispose()
                self._engine = None
                self.logger.info("Disposed the engine.")

    def __enter__(self):
        """
        Start an synchronous database engine upon entry.
        """
        if self._driver_is_async:
            raise RuntimeError(
                f"{self._rendered_url.drivername} cannot be run synchronously. "
                f"Please use the `async with` syntax."
            )
        return self

    def __exit__(self, *args):
        """
        Dispose the synchronous database engine upon exit.
        """
        self.close()

    def close(self):
        """
        Closes sync connections and its cursors.
        """
        if self._driver_is_async:
            raise RuntimeError(
                f"{self._rendered_url.drivername} is not synchronous. "
                f"Please use the `aclose` method instead."
            )

        try:
            self.reset_connections()
        finally:
            if self._engine is not None:
                self._engine.dispose()
                self._engine = None
                self.logger.info("Disposed the engine.")

    def __getstate__(self):
        """Allows the block to be pickleable."""
        data = self.__dict__.copy()
        data.update({k: None for k in {"_engine", "_exit_stack", "_unique_results"}})
        return data

    def __setstate__(self, data: dict):
        """Upon loading back, restart the engine and results."""
        self.__dict__.update(data)
        self._start_engine()  # block initialization doesn't get called here
        self._start_exit_stack()
        self._unique_results = {}
