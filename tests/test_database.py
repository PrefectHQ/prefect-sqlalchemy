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


@pytest.fixture()
def sqlalchemy_credentials():
    sqlalchemy_credentials_mock = MagicMock()
    sqlalchemy_credentials_mock.get_connection.return_value = SQLAlchemyConnectionMock()
    return sqlalchemy_credentials_mock


@pytest.mark.parametrize("limit", [None, 3])
def test_sqlalchemy_query(limit, sqlalchemy_credentials):
    @flow
    def test_flow():
        result = sqlalchemy_query(
            "query", sqlalchemy_credentials, params=("param",), limit=limit
        )
        return result

    result = test_flow().result().result()
    assert str(result[0][0]) == "query"
    assert result[0][1] == ("param",)
    if limit is None:
        assert len(result) == 1
    else:
        assert len(result) == limit


def test_sqlalchemy_execute(sqlalchemy_credentials):
    @flow
    def test_flow():
        result = sqlalchemy_execute("query", sqlalchemy_credentials)
        return result

    result = test_flow().result().result()
    assert result is None
