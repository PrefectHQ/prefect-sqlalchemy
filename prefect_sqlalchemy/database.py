"""Tasks for querying a database with SQLAlchemy"""

import contextlib
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from prefect import task
from sqlalchemy.sql import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine
    from sqlalchemy.engine.cursor import CursorResult
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

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
