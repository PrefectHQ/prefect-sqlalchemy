from contextlib import asynccontextmanager, contextmanager
from unittest.mock import MagicMock

import pytest
from prefect import flow

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

    result = (await test_flow()).result().result()
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

    result = test_flow().result().result()
    assert str(result[0][0]) == "query"
    assert result[0][1] == ("param",)
    if limit is None:
        assert len(result) == 1
    else:
        assert len(result) == limit


@pytest.mark.parametrize("dispose", [True, False])
async def test_sqlalchemy_execute_async(sqlalchemy_credentials_async, dispose):
    @flow
    async def test_flow():
        result = await sqlalchemy_execute(
            "statement", sqlalchemy_credentials_async, dispose=dispose
        )
        return result

    result = ((await test_flow()).result()).result()
    assert result is None


@pytest.mark.parametrize("dispose", [True, False])
def test_sqlalchemy_execute_sync(sqlalchemy_credentials_sync, dispose):
    @flow
    def test_flow():
        result = sqlalchemy_execute(
            "statement", sqlalchemy_credentials_sync, dispose=dispose
        )
        return result

    result = test_flow().result().result()
    assert result is None
