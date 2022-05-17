import pytest
from prefect import flow
from sqlalchemy.engine import URL, Engine
from sqlalchemy.ext.asyncio.engine import AsyncEngine

from prefect_sqlalchemy.credentials import AsyncDriver, DatabaseCredentials, SyncDriver


@pytest.mark.parametrize(
    "url_param", ["driver", "username", "password", "database", "host", "port", "query"]
)
def test_sqlalchemy_credentials_post_init_url_param_conflict(url_param):
    @flow
    def test_flow():
        url_params = {url_param: url_param}
        with pytest.raises(
            ValueError, match="The `url` should not be provided alongside"
        ):
            DatabaseCredentials(url="url", **url_params)

    test_flow().result()


@pytest.mark.parametrize("url_param", ["driver", "username", "database"])
def test_sqlalchemy_credentials_post_init_url_param_missing(url_param):
    @flow
    def test_flow():
        url_params = {
            "driver": "driver",
            "username": "username",
            "database": "database",
        }
        url_params.pop(url_param)
        with pytest.raises(ValueError, match="If the `url` is not provided"):
            DatabaseCredentials(**url_params)

    test_flow().result()


@pytest.mark.parametrize(
    "driver", [AsyncDriver.POSTGRESQL_ASYNCPG, "postgresql+asyncpg"]
)
def test_sqlalchemy_credentials_get_engine_async(driver):
    @flow
    def test_flow():
        sqlalchemy_credentials = DatabaseCredentials(
            driver, "user", "password", "database", host="localhost", port=5432
        )
        assert sqlalchemy_credentials._async_supported is True

        expected_url = "postgresql+asyncpg://user:***@localhost:5432/database"
        assert repr(sqlalchemy_credentials.url) == expected_url
        assert isinstance(sqlalchemy_credentials.url, URL)

        engine = sqlalchemy_credentials.get_engine()
        assert engine.url.render_as_string() == expected_url
        assert isinstance(engine, AsyncEngine)

    test_flow().result()


@pytest.mark.parametrize(
    "driver", [SyncDriver.POSTGRESQL_PSYCOPG2, "postgresql+psycopg2"]
)
def test_sqlalchemy_credentials_get_engine_sync(driver):
    @flow
    def test_flow():
        sqlalchemy_credentials = DatabaseCredentials(
            driver, "user", "password", "database", host="localhost", port=5432
        )
        assert sqlalchemy_credentials._async_supported is False

        expected_url = "postgresql+psycopg2://user:***@localhost:5432/database"
        assert repr(sqlalchemy_credentials.url) == expected_url
        assert isinstance(sqlalchemy_credentials.url, URL)

        engine = sqlalchemy_credentials.get_engine()
        assert engine.url.render_as_string() == expected_url
        assert isinstance(engine, Engine)

    test_flow().result()


@pytest.mark.parametrize("url_type", [str, URL])
def test_sqlalchemy_credentials_get_engine_url(url_type):
    @flow
    def test_flow():
        if isinstance(url_type, str):
            url = "postgresql://username:password@account/database"
        else:
            url = URL.create(
                "postgresql",
                "username",
                "password",
                host="account",
                database="database",
            )
        sqlalchemy_credentials = DatabaseCredentials(url=url)
        assert sqlalchemy_credentials._async_supported is False

        expected_url = "postgresql://username:***@account/database"
        assert repr(sqlalchemy_credentials.url) == expected_url
        assert isinstance(sqlalchemy_credentials.url, URL)

        engine = sqlalchemy_credentials.get_engine()
        assert engine.url.render_as_string() == expected_url
        assert isinstance(engine, Engine)

    test_flow().result()
