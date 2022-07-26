"""Tasks for querying a database with SQLAlchemy"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from prefect import task
from sqlalchemy.sql import text

if TYPE_CHECKING:
    from sqlalchemy.engine.cursor import CursorResult

    from prefect_sqlalchemy.credentials import DatabaseCredentials


async def _execute(
    query: str,
    sqlalchemy_credentials: "DatabaseCredentials",
    params: Optional[Union[Tuple[Any], Dict[str, Any]]] = None,
) -> "CursorResult":
    """
    Executes a SQL query.
    """
    engine = sqlalchemy_credentials.get_engine()
    try:
        execute_args = (text(query), params)
        if sqlalchemy_credentials._async_supported:
            async with engine.connect() as connection:
                result = await connection.execute(*execute_args)
                await connection.commit()
        else:
            with engine.connect() as connection:
                result = connection.execute(*execute_args)
                # commit is not available
    finally:
        if sqlalchemy_credentials._async_supported:
            await engine.dispose()
        else:
            engine.dispose()
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
                driver=AsyncDriver.POSTGRESQL_ASYNCPG,
                username="prefect",
                password="prefect_password",
                database="postgres",
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
    await _execute(statement, sqlalchemy_credentials, params=params)


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
                driver=AsyncDriver.POSTGRESQL_ASYNCPG,
                username="prefect",
                password="prefect_password",
                database="postgres",
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
    result = await _execute(query, sqlalchemy_credentials, params=params)
    if limit is None:
        return result.fetchall()
    else:
        return result.fetchmany(limit)
