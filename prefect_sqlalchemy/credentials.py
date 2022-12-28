"""Credential classes used to perform authenticated interactions with SQLAlchemy"""

from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

from prefect.blocks.core import Block
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from prefect.utilities.hashing import hash_objects
from pydantic import AnyUrl, Field, SecretStr
from sqlalchemy.engine import Connection, Engine, create_engine
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
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

    Upon instantiating, a connection to the database is established
    and maintained for the life of the object until the close method is called.

    It is recommended to use this block as a context manager, which will automatically
    close the connection and its cursors when the context is exited.

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
        Load stored database credentials:
        ```python
        from prefect_sqlalchemy import DatabaseCredentials

        database_block = DatabaseCredentials.load("BLOCK_NAME")
        ```

        Create table named customers and insert values; then fetch the first 10 rows.
        ```python
        from prefect import flow
        from prefect_sqlalchemy import Database

        @flow
        def database_flow():
            database = Database.load("database")
            database.execute(
                "CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);",
            )
            for i in range(1, 42):
                database.execute(
                    "INSERT INTO customers (name, address) VALUES (:name, :address);",
                    parameters={"name": "Marvin", "address": f"Highway {i}"},
                )
            return database.fetch_many(
                "SELECT * FROM customers WHERE name = :name;",
                parameters={"name": "Marvin"},
            )

        database_flow()
        ```
    """

    _block_type_name = "Database Credentials"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/3xLant5G70S4vJpmdWCYmr/8fdb19f15b97c3a07c3af3efde4d28fb/download.svg.png?h=250"  # noqa

    driver: Optional[Union[AsyncDriver, SyncDriver, str]] = Field(
        default=None, description="The driver name to use."
    )
    username: Optional[str] = Field(
        default=None, description="The user name used to authenticate."
    )
    password: Optional[SecretStr] = Field(
        default=None, description="The password used to authenticate."
    )
    database: Optional[str] = Field(
        default=None, description="The name of the database to use."
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
    url: Optional[AnyUrl] = Field(
        default=None,
        description=(
            "Manually create and provide a URL to create the engine, this is useful "
            "for external dialects, e.g. Snowflake, because some of the params, "
            "such as 'warehouse', is not directly supported in the vanilla "
            "`sqlalchemy.engine.URL.create` method; do not provide this "
            "alongside with other URL params as it will raise a `ValueError`."
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
    _unique_results: Dict[str, CursorResult] = None

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

    def block_initialization(self):
        """
        Initializes the engine.
        """
        super().block_initialization()

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

        if self._egnine is None:
            self._start_engine()

        if self._unique_results is None:
            self._unique_results = {}

    def _start_engine(self):
        """
        Starts SQLAlchemy database engine.
        """
        self._engine = self.get_engine()

    def get_engine(self) -> Union[Engine, AsyncEngine]:
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
        if self._engine is not None:
            return self._engine

        engine_kwargs = dict(
            url=self.rendered_url,
            connect_args=self.connect_args or {},
        )
        if self._async_supported:
            engine = create_async_engine(**engine_kwargs)
        else:
            engine = create_engine(**engine_kwargs)
        return engine

    def get_connection(self, begin: bool = True) -> Union[Connection, AsyncConnection]:
        """
        Returns an authenticated connection that can be used to query from databases.
        Args:
            begin: Whether to begin a transaction on the connection; if True, if
                any operations fail, the entire transaction will be rolled back.

        Returns:
            The authenticated SQLAlchemy Connection / AsyncConnection.

        Examples:
            Create an synchronous connection as a context-managed transaction.
            ```python
            from prefect import flow
            from prefect_sqlalchemy import DatabaseCredentials
            @flow
            def get_connection_flow():
                database_credentials = DatabaseCredentials.load("BLOCK_NAME")
                with database_credentials.get_connection(begin=True) as connection:
                    connection.execute("SELECT * FROM table LIMIT 1;")
            ```

            Create an asynchronous connection as a context-managed transacation.
            ```python
            from prefect import flow
            from prefect_sqlalchemy import DatabaseCredentials
            @flow
            def get_connection_flow():
                database_credentials = DatabaseCredentials.load("BLOCK_NAME")
                async with database_credentials.get_connection(begin=True) as connection:
                    await connection.execute("SELECT * FROM table LIMIT 1;")
            ```
        """  # noqa: E501
        engine = self.get_engine()
        if begin:
            return engine.begin()
        else:
            return engine.connect()

    async def _async_execute(
        self, *execute_args: Tuple[Any], **execute_kwargs: Dict[str, Any]
    ) -> CursorResult:
        """
        Execute the statement asynchronously.
        """
        connection = self._engine.connect()

        execution_options = execute_kwargs.pop("execution_options")
        connection.execution_options(**execution_options)  # modified in place

        if self._async_supported:
            result_set = await connection.execute(*execute_args, **execute_kwargs)
        else:
            result_set = await run_sync_in_worker_thread(
                connection.execute, *execute_args, **execute_kwargs
            )
        return result_set

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
            result_set = self._async_execute(*execute_args, **execute_kwargs)
            self._unique_results[input_hash] = result_set
        else:
            result_set = self._unique_results[input_hash]
        return result_set

    def reset_connections(self) -> None:
        """
        Tries to close all opened connections and their results.
        """
        input_hashes = tuple(self._unique_results.keys())
        for input_hash in input_hashes:
            connection = self._unique_results.pop(input_hash).connection
            try:
                connection.close()
            except Exception as exc:
                self.logger.warning(
                    f"Failed to close connection for input hash {input_hash!r}: {exc}"
                )

    @sync_compatible
    async def fetch_one(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        **execution_options: Dict[str, Any],
    ) -> Tuple[Any]:
        """
        Fetch a single result from the database.

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            **execution_options: Additional options to pass to `connection.execute`.

        Returns:
            A list of tuples containing the data returned by the database,
                where each row is a tuple and each column is a value in the tuple.
        """
        inputs = dict(
            statement=text(operation),
            parameters=parameters,
            execution_options=execution_options,
        )
        result_set = await self._get_result_set(**inputs)
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

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            size: The number of results to return; if None or 0, uses the value of
                `fetch_size` configured on the block.
            **execution_options: Additional options to pass to `connection.execute`.

        Returns:
            A list of tuples containing the data returned by the database,
                where each row is a tuple and each column is a value in the tuple.
        """
        inputs = dict(
            statement=text(operation),
            parameters=parameters,
            execution_options=execution_options,
        )
        result_set = await self._get_result_set(**inputs)
        size = size or self.fetch_size
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

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            **execution_options: Additional options to pass to `connection.execute`.

        Returns:
            A list of tuples containing the data returned by the database,
                where each row is a tuple and each column is a value in the tuple.
        """
        inputs = dict(
            statement=text(operation),
            parameters=parameters,
            execution_options=execution_options,
        )
        result_set = await self._get_result_set(**inputs)
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

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            **execution_options: Additional options to pass to `connection.execute`.
        """
        inputs = dict(
            statement=text(operation),
            parameters=parameters,
            execution_options=execution_options,
        )
        await self._async_execute(**inputs)

    @sync_compatible
    async def execute_many(
        self,
        operation: str,
        seq_of_parameters: Optional[List[Dict[str, Any]]],
        **execution_options: Dict[str, Any],
    ) -> None:
        """
        Executes many operations on the database. This method is intended to be used
        for operations that do not return data, such as INSERT, UPDATE, or DELETE.

        Args:
            operation: The SQL query or other operation to be executed.
            seq_of_parameters: The sequence of parameters for the operation.
            **execution_options: Additional options to pass to `connection.execute`.
        """
        await self._async_execute(
            text(operation),
            seq_of_parameters,  # multiparams has to be positional
            execution_options=execution_options,
        )

    @sync_compatible
    async def close(self):
        """
        Closes connection and its cursors.
        """
        try:
            self.reset_connections()
        finally:
            if self._engine is not None:
                dispose = self._engine.dispose()
                if self._async_supported:
                    await dispose
                self._engine = None

    async def __aenter__(self):
        """
        Start an asynchronous database engine upon entry.
        """
        if not self._async_supported:
            raise ValueError(
                f"{self.driver} does not support asynchronous execution. "
                f"Please use the `with` syntax."
            )
        # no need to await here
        self._engine = self.get_engine()
        return self

    async def __aexit__(self, *args):
        """
        Dispose the asynchronous database engine upon exit.
        """
        await self.close()

    def __enter__(self):
        """
        Start an synchronous database engine upon entry.
        """
        if self._async_supported:
            raise ValueError(
                f"{self.driver} does not support synchronous execution. "
                f"Please use the `async with` syntax."
            )
        self._engine = self.get_engine()
        return self

    def __exit__(self, *args):
        """
        Dispose the synchronous database engine upon exit.
        """
        self.close()

    def __getstate__(self):
        """Allows the block to be pickleable."""
        data = self.__dict__.copy()
        data.update({k: None for k in {"_engine", "_unique_results"}})
        return data

    def __setstate__(self, data: dict):
        """Upon loading back, restart the engine and results."""
        self.__dict__.update(data)
        self._unique_results = {}
        self.get_engine()
