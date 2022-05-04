from unittest.mock import MagicMock

import pytest
from prefect import flow

from prefect_sqlalchemy.database import sqlalchemy_execute, sqlalchemy_query


class SQLAlchemyConnectionMock:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, query, params):
        cursor_result = MagicMock()
        cursor_result.fetchall.side_effect = [(query, params)]
        return cursor_result

    async def commit(self):
        pass


@pytest.fixture()
def sqlalchemy_credentials():
    sqlalchemy_credentials_mock = MagicMock()
    sqlalchemy_credentials_mock.get_connection.return_value = SQLAlchemyConnectionMock()
    return sqlalchemy_credentials_mock


def test_sqlalchemy_query(sqlalchemy_credentials):
    @flow
    def test_flow():
        result = sqlalchemy_query(
            "query",
            sqlalchemy_credentials,
            params=("param",),
        )
        return result

    result = test_flow().result().result()
    assert str(result[0]) == "query"
    assert result[1] == ("param",)


def test_sqlalchemy_execute(sqlalchemy_credentials):
    @flow
    def test_flow():
        result = sqlalchemy_execute("query", sqlalchemy_credentials)
        return result

    result = test_flow().result().result()
    assert result is None
