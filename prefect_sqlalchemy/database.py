"""Tasks for querying a database with SQLAlchemy"""

import contextlib
from typing import Any, Dict, List, Optional, Tuple, Union

from prefect import task
from prefect.blocks.abstract import DatabaseBlock
from prefect.utilities.asyncutils import sync_compatible
from pydantic import Field
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.sql import text

from prefect_sqlalchemy.credentials import DatabaseCredentials


@contextlib.asynccontextmanager
async def _connect(
    engine: Union["AsyncEngine", "Engine"],
    async_supported: bool,
) -> Union["AsyncConnection", "Connection"]:
    """
    Helper method to create a connection to the database, either
    synchronously or asynchronously.
    """
    try:
        # a context manager nested within a context manager!
        if async_supported:
            async with engine.connect() as connection:
                yield connection
        else:
            with engine.connect() as connection:
                yield connection
    finally:
        dispose = engine.dispose()
        if async_supported:
            await dispose


async def _execute(
    connection: Union["AsyncConnection", "Connection"],
    query: str,
    params: Optional[Union[Tuple[Any], Dict[str, Any]]],
    async_supported: bool,
) -> "CursorResult":
    """
    Helper method to execute database queries or statements, either
    synchronously or asynchronously.
    """
    result = connection.execute(text(query), params)
    if async_supported:
        result = await result
        await connection.commit()
    return result


@task
async def sqlalchemy_execute(
    statement: str,
    sqlalchemy_credentials: "DatabaseCredentials",
    params: Optional[Union[Tuple[Any], Dict[str, Any]]] = None,
):
    """
    Executes a SQL DDL or DML statement; useful for creating tables and inserting rows
    since this task does not return any objects.

    Args:
        statement: The statement to execute against the database.
        sqlalchemy_credentials: The credentials to use to authenticate.
        params: The params to replace the placeholders in the query.

    Examples:
        Create table named customers and insert values.
        ```python
        from prefect_sqlalchemy import DatabaseCredentials, AsyncDriver
        from prefect_sqlalchemy.database import sqlalchemy_execute
        from prefect import flow

        @flow
        def sqlalchemy_execute_flow():
            sqlalchemy_credentials = DatabaseCredentials(
                driver=AsyncDriver.SQLITE_AIOSQLITE,
                database="prefect.db",
            )
            sqlalchemy_execute(
                "CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);",
                sqlalchemy_credentials,
            )
            sqlalchemy_execute(
                "INSERT INTO customers (name, address) VALUES (:name, :address);",
                sqlalchemy_credentials,
                params={"name": "Marvin", "address": "Highway 42"}
            )

        sqlalchemy_execute_flow()
        ```
    """
    # do not return anything or else results in the error:
    # This result object does not return rows. It has been closed automatically
    engine = sqlalchemy_credentials.get_engine()
    async_supported = sqlalchemy_credentials._async_supported
    async with _connect(engine, async_supported) as connection:
        await _execute(connection, statement, params, async_supported)


@task
async def sqlalchemy_query(
    query: str,
    sqlalchemy_credentials: "DatabaseCredentials",
    params: Optional[Union[Tuple[Any], Dict[str, Any]]] = None,
    limit: Optional[int] = None,
) -> List[Tuple[Any]]:
    """
    Executes a SQL query; useful for querying data from existing tables.

    Args:
        query: The query to execute against the database.
        sqlalchemy_credentials: The credentials to use to authenticate.
        params: The params to replace the placeholders in the query.
        limit: The number of rows to fetch. Note, this parameter is
            executed on the client side, i.e. passed to `fetchmany`.
            To limit on the server side, add the `LIMIT` clause, or
            the dialect's equivalent clause, like `TOP`, to the query.

    Returns:
        The fetched results.

    Examples:
        Query postgres table with the ID value parameterized.
        ```python
        from prefect_sqlalchemy import DatabaseCredentials, AsyncDriver
        from prefect_sqlalchemy.database import sqlalchemy_query
        from prefect import flow

        @flow
        def sqlalchemy_query_flow():
            sqlalchemy_credentials = DatabaseCredentials(
                driver=AsyncDriver.SQLITE_AIOSQLITE,
                database="prefect.db",
            )
            result = sqlalchemy_query(
                "SELECT * FROM customers WHERE name = :name;",
                sqlalchemy_credentials,
                params={"name": "Marvin"},
            )
            return result

        sqlalchemy_query_flow()
        ```
    """
    engine = sqlalchemy_credentials.get_engine()
    async_supported = sqlalchemy_credentials._async_supported
    async with _connect(engine, async_supported) as connection:
        result = await _execute(connection, query, params, async_supported)
        # some databases, like sqlite, require a connection still open to fetch!
        rows = result.fetchall() if limit is None else result.fetchmany(limit)
    return rows


class Database(DatabaseBlock):
    """
    A block for querying a database with SQLAlchemy.

    Attributes:
        database_credentials: The credentials to use to authenticate.
        fetch_size: The number of rows to fetch at a time when calling fetch_many.
            Note, this parameter is executed on the client side and is not
            passed to the database. To limit on the server side, add the `LIMIT`
            clause, or the dialect's equivalent clause, like `TOP`, to the query.

    Examples:
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

    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/3xLant5G70S4vJpmdWCYmr/8fdb19f15b97c3a07c3af3efde4d28fb/download.svg.png?h=250"  # noqa

    database_credentials: "DatabaseCredentials"
    fetch_size: int = Field(
        default=1, description="The number of rows to fetch at a time."
    )

    @contextlib.asynccontextmanager
    async def _async_or_sync_execute(
        self,
        operation: str,
        parameters: Optional[Union[Tuple[Any], Dict[str, Any]]] = None,
        **execution_options: Dict[str, Any]
    ) -> CursorResult:
        """
        Helper method to execute database queries or statements, either
        synchronously or asynchronously.
        """
        credentials = self.database_credentials
        async with credentials._async_or_sync_connect() as connection:
            result = connection.execute(
                text(operation), parameters, execution_options=execution_options
            )
            if credentials._async_supported:
                result = await result
                await connection.commit()
            yield result

    @sync_compatible
    async def fetch_one(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        **execution_options: Dict[str, Any]
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
        async with self._async_or_sync_execute(
            operation, parameters, **execution_options
        ) as result:
            return result.fetchone()

    @sync_compatible
    async def fetch_many(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        size: Optional[int] = None,
        **execution_options: Dict[str, Any]
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
        async with self._async_or_sync_execute(
            operation, parameters, **execution_options
        ) as result:
            size = size or self.fetch_size
            return result.fetchmany(size=size)

    @sync_compatible
    async def fetch_all(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        **execution_options: Dict[str, Any]
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
        async with self._async_or_sync_execute(
            operation, parameters, **execution_options
        ) as result:
            return result.fetchall()

    @sync_compatible
    async def execute(
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        **execution_options: Dict[str, Any]
    ) -> None:
        """
        Executes an operation on the database. This method is intended to be used
        for operations that do not return data, such as INSERT, UPDATE, or DELETE.

        Args:
            operation: The SQL query or other operation to be executed.
            parameters: The parameters for the operation.
            **execution_options: Additional options to pass to `connection.execute`.
        """
        async with self._async_or_sync_execute(
            operation, parameters, **execution_options
        ) as result:
            return result

    @sync_compatible
    async def execute_many(
        self,
        operation: str,
        seq_of_parameters: Optional[List[Dict[str, Any]]],
        **execution_options: Dict[str, Any]
    ) -> None:
        """
        Executes many operations on the database. This method is intended to be used
        for operations that do not return data, such as INSERT, UPDATE, or DELETE.

        Args:
            operation: The SQL query or other operation to be executed.
            seq_of_parameters: The sequence of parameters for the operation.
            **execution_options: Additional options to pass to `connection.execute`.
        """
        async with self._async_or_sync_execute(
            operation, seq_of_parameters, **execution_options
        ) as result:
            return result
