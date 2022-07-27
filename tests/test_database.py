from contextlib import asynccontextmanager, contextmanager
from unittest.mock import MagicMock

import pytest
from prefect import flow

from prefect_sqlalchemy.credentials import AsyncDriver, DatabaseCredentials, SyncDriver
from prefect_sqlalchemy.database import sqlalchemy_execute, sqlalchemy_query


class SQLAlchemyAsyncEngineMock:
    async def dispose(self):
        return True

    @asynccontextmanager
    async def connect(self):
        try:
            yield SQLAlchemyAsyncConnectionMock()
        finally:
            pass


class SQLAlchemyAsyncConnectionMock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, query, params):
        cursor_result = MagicMock()
        cursor_result.fetchall.side_effect = lambda: [
            (query, params),
        ]
        cursor_result.fetchmany.side_effect = (
            lambda size: [
                (query, params),
            ]
            * size
        )
        return cursor_result

    async def commit(self):
        pass


class SQLAlchemyEngineMock:
    def dispose(self):
        return True

    @contextmanager
    def connect(self):
        try:
            yield SQLAlchemyConnectionMock()
        finally:
            pass


class SQLAlchemyConnectionMock:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params):
        cursor_result = MagicMock()
        cursor_result.fetchall.side_effect = lambda: [
            (query, params),
        ]
        cursor_result.fetchmany.side_effect = (
            lambda size: [
                (query, params),
            ]
            * size
        )
        return cursor_result


@pytest.fixture()
def sqlalchemy_credentials_async():
    sqlalchemy_credentials_mock = MagicMock()
    sqlalchemy_credentials_mock._async_supported = True
    sqlalchemy_credentials_mock.get_engine.return_value = SQLAlchemyAsyncEngineMock()
    return sqlalchemy_credentials_mock


@pytest.fixture()
def sqlalchemy_credentials_sync():
    sqlalchemy_credentials_mock = MagicMock()
    sqlalchemy_credentials_mock._async_supported = False
    sqlalchemy_credentials_mock.get_engine.return_value = SQLAlchemyEngineMock()
    return sqlalchemy_credentials_mock


@pytest.mark.parametrize("limit", [None, 3])
async def test_sqlalchemy_query_async(limit, sqlalchemy_credentials_async):
    @flow
    async def test_flow():
        result = await sqlalchemy_query(
            "query", sqlalchemy_credentials_async, params=("param",), limit=limit
        )
        return result

    result = await test_flow()
    assert str(result[0][0]) == "query"
    assert result[0][1] == ("param",)
    if limit is None:
        assert len(result) == 1
    else:
        assert len(result) == limit


@pytest.mark.parametrize("limit", [None, 3])
def test_sqlalchemy_query_sync(limit, sqlalchemy_credentials_sync):
    @flow
    def test_flow():
        result = sqlalchemy_query(
            "query", sqlalchemy_credentials_sync, params=("param",), limit=limit
        )
        return result

    result = test_flow()
    assert str(result[0][0]) == "query"
    assert result[0][1] == ("param",)
    if limit is None:
        assert len(result) == 1
    else:
        assert len(result) == limit


async def test_sqlalchemy_execute_async(sqlalchemy_credentials_async):
    @flow
    async def test_flow():
        result = await sqlalchemy_execute.submit(
            "statement", sqlalchemy_credentials_async
        )
        return result

    result = await test_flow()
    assert result.result() is None


def test_sqlalchemy_execute_sync(sqlalchemy_credentials_sync):
    @flow
    def test_flow():
        result = sqlalchemy_execute.submit("statement", sqlalchemy_credentials_sync)
        return result

    result = test_flow()
    assert result.result() is None


def test_sqlalchemy_execute_twice_no_error(sqlalchemy_credentials_sync):
    @flow
    def test_flow():
        result = sqlalchemy_execute.submit("statement", sqlalchemy_credentials_sync)
        result = sqlalchemy_execute.submit("statement", sqlalchemy_credentials_sync)
        return result

    result = test_flow()
    assert result.result() is None


def test_sqlalchemy_execute_sqlite(tmp_path):
    @flow
    def sqlalchemy_execute_flow():
        sqlalchemy_credentials = DatabaseCredentials(
            driver=SyncDriver.SQLITE_PYSQLITE,
            database=str(tmp_path / "prefect.db"),
        )
        sqlalchemy_execute(
            "CREATE TABLE customers (name varchar, address varchar);",
            sqlalchemy_credentials,
        )
        sqlalchemy_execute(
            "INSERT INTO customers (name, address) VALUES (:name, :address);",
            sqlalchemy_credentials,
            params={"name": "Marvin", "address": "Highway 42"},
        )
        result = sqlalchemy_query(
            "SELECT * FROM customers WHERE name = :name;",
            sqlalchemy_credentials,
            params={"name": "Marvin"},
        )
        return result

    rows = sqlalchemy_execute_flow()
    assert len(rows) == 1

    row = rows[0]
    assert len(row) == 2
    assert row[0] == "Marvin"
    assert row[1] == "Highway 42"


async def test_sqlalchemy_execute_sqlite_async(tmp_path):
    @flow
    async def sqlalchemy_execute_flow():
        sqlalchemy_credentials = DatabaseCredentials(
            driver=AsyncDriver.SQLITE_AIOSQLITE,
            database=str(tmp_path / "prefect_async.db"),
        )
        await sqlalchemy_execute(
            "CREATE TABLE customers (name varchar, address varchar);",
            sqlalchemy_credentials,
        )
        await sqlalchemy_execute(
            "INSERT INTO customers (name, address) VALUES (:name, :address);",
            sqlalchemy_credentials,
            params={"name": "Marvin", "address": "Highway 42"},
        )
        await sqlalchemy_execute(
            "INSERT INTO customers (name, address) VALUES (:name, :address);",
            sqlalchemy_credentials,
            params={"name": "Ford", "address": "Highway 42"},
        )
        result = await sqlalchemy_query(
            "SELECT * FROM customers WHERE address = :address;",
            sqlalchemy_credentials,
            params={"address": "Highway 42"},
        )
        return result

    rows = await sqlalchemy_execute_flow()
    assert len(rows) == 2

    expected_names = {"Ford", "Marvin"}
    expected_address = "Highway 42"
    actual_names, actual_addresses = zip(*rows)
    assert expected_names == set(actual_names)
    assert expected_address == actual_addresses[0]
