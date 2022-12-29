import cloudpickle
import pytest
from prefect import flow
from sqlalchemy.engine import URL, Connection, Engine
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from prefect_sqlalchemy.credentials import (
    AsyncDriver,
    DatabaseCredentials,
    SqlAlchemyConnector,
    SqlAlchemyUrl,
    SyncDriver,
)


@pytest.mark.parametrize(
    "url_param", ["driver", "username", "password", "database", "host", "port", "query"]
)
def test_sqlalchemy_credentials_post_init_url_param_conflict(url_param):
    @flow
    def test_flow():
        url_params = {url_param: url_param}
        if url_param == "query":
            url_params["query"] = {"query": "query"}
        with pytest.raises(
            ValueError, match="The `url` should not be provided alongside"
        ):
            DatabaseCredentials(
                url="postgresql+asyncpg://user:password@localhost:5432/database",
                **url_params,
            )

    test_flow()


@pytest.mark.parametrize("url_param", ["driver", "database"])
def test_sqlalchemy_credentials_post_init_url_param_missing(url_param):
    @flow
    def test_flow():
        url_params = {
            "driver": "driver",
            "database": "database",
        }
        url_params.pop(url_param)
        with pytest.raises(ValueError, match="If the `url` is not provided"):
            DatabaseCredentials(**url_params)

    test_flow()


@pytest.mark.parametrize(
    "driver", [AsyncDriver.POSTGRESQL_ASYNCPG, "postgresql+asyncpg"]
)
def test_sqlalchemy_credentials_get_engine_async(driver):
    @flow
    def test_flow():
        sqlalchemy_credentials = DatabaseCredentials(
            driver=driver,
            username="user",
            password="password",
            database="database",
            host="localhost",
            port=5432,
        )
        assert sqlalchemy_credentials._driver_is_async is True
        assert sqlalchemy_credentials.url is None

        expected_rendered_url = "postgresql+asyncpg://user:***@localhost:5432/database"
        assert repr(sqlalchemy_credentials.rendered_url) == expected_rendered_url
        assert isinstance(sqlalchemy_credentials.rendered_url, URL)

        engine = sqlalchemy_credentials.get_engine()
        assert engine.url.render_as_string() == expected_rendered_url
        assert isinstance(engine, AsyncEngine)

    test_flow()


@pytest.mark.parametrize(
    "driver", [SyncDriver.POSTGRESQL_PSYCOPG2, "postgresql+psycopg2"]
)
def test_sqlalchemy_credentials_get_engine_sync(driver):
    @flow
    def test_flow():
        sqlalchemy_credentials = DatabaseCredentials(
            driver=driver,
            username="user",
            password="password",
            database="database",
            host="localhost",
            port=5432,
        )
        assert sqlalchemy_credentials._driver_is_async is False
        assert sqlalchemy_credentials.url is None

        expected_rendered_url = "postgresql+psycopg2://user:***@localhost:5432/database"
        assert repr(sqlalchemy_credentials.rendered_url) == expected_rendered_url
        assert isinstance(sqlalchemy_credentials.rendered_url, URL)

        engine = sqlalchemy_credentials.get_engine()
        assert engine.url.render_as_string() == expected_rendered_url
        assert isinstance(engine, Engine)

    test_flow()


def test_sqlalchemy_credentials_get_engine_url():
    @flow
    def test_flow():
        url = "postgresql://username:password@account/database"
        sqlalchemy_credentials = DatabaseCredentials(url=url)
        assert sqlalchemy_credentials._driver_is_async is False
        assert sqlalchemy_credentials.url == url

        expected_rendered_url = "postgresql://username:***@account/database"
        assert repr(sqlalchemy_credentials.rendered_url) == expected_rendered_url
        assert isinstance(sqlalchemy_credentials.rendered_url, URL)

        engine = sqlalchemy_credentials.get_engine()
        assert engine.url.render_as_string() == expected_rendered_url
        assert isinstance(engine, Engine)

    test_flow()


def test_sqlalchemy_credentials_sqlite(tmp_path):
    @flow
    def test_flow():
        driver = SyncDriver.SQLITE_PYSQLITE
        database = str(tmp_path / "prefect.db")
        sqlalchemy_credentials = DatabaseCredentials(driver=driver, database=database)
        assert sqlalchemy_credentials._driver_is_async is False

        expected_rendered_url = f"sqlite+pysqlite:///{database}"
        assert repr(sqlalchemy_credentials.rendered_url) == expected_rendered_url
        assert isinstance(sqlalchemy_credentials.rendered_url, URL)

        engine = sqlalchemy_credentials.get_engine()
        assert engine.url.render_as_string() == expected_rendered_url
        assert isinstance(engine, Engine)

    test_flow()


def test_save_load_roundtrip():
    """
    This test was added because there was an issue saving rendered_url;
    sqlalchemy.engine.url.URL was not JSON serializable.
    """
    credentials = DatabaseCredentials(
        driver=AsyncDriver.POSTGRESQL_ASYNCPG,
        username="test-username",
        password="test-password",
        database="test-database",
        host="localhost",
        port="5678",
    )
    credentials.save("test-credentials", overwrite=True)
    loaded_credentials = credentials.load("test-credentials")
    assert loaded_credentials.driver == AsyncDriver.POSTGRESQL_ASYNCPG
    assert loaded_credentials.username == "test-username"
    assert loaded_credentials.password.get_secret_value() == "test-password"
    assert loaded_credentials.database == "test-database"
    assert loaded_credentials.host == "localhost"
    assert loaded_credentials.port == "5678"
    assert loaded_credentials.rendered_url == credentials.rendered_url


class TestSqlAlchemyConnector:
    @pytest.fixture(params=[SyncDriver.SQLITE_PYSQLITE, AsyncDriver.SQLITE_AIOSQLITE])
    async def connector_with_data(self, tmp_path, request):
        credentials = SqlAlchemyConnector(
            url=SqlAlchemyUrl(
                driver=request.param,
                database=str(tmp_path / "test.db"),
            ),
            fetch_size=2,
        )
        await credentials.execute(
            "CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);"
        )
        await credentials.execute(
            "INSERT INTO customers (name, address) VALUES (:name, :address);",
            parameters={"name": "Marvin", "address": "Highway 42"},
        )
        await credentials.execute_many(
            "INSERT INTO customers (name, address) VALUES (:name, :address);",
            seq_of_parameters=[
                {"name": "Ford", "address": "Highway 42"},
                {"name": "Unknown", "address": "Space"},
                {"name": "Me", "address": "Myway 88"},
            ],
        )
        yield credentials

    @pytest.fixture(params=[True, False])
    async def managed_connector_with_data(self, connector_with_data, request):
        if request.param:
            if connector_with_data._driver_is_async:
                async with connector_with_data:
                    yield connector_with_data
            else:
                with connector_with_data:
                    yield connector_with_data
        else:
            yield connector_with_data
            if connector_with_data._driver_is_async:
                await connector_with_data.aclose()
            else:
                connector_with_data.close()
        assert connector_with_data._unique_results == {}
        assert connector_with_data._engine is None

    @pytest.mark.parametrize("begin", [True, False])
    def test_get_connection(self, begin, managed_connector_with_data):
        connection = managed_connector_with_data.get_connection(begin=begin)
        if begin:
            engine_type = (
                AsyncEngine if managed_connector_with_data._driver_is_async else Engine
            )
            assert isinstance(connection, engine_type._trans_ctx)
        else:
            engine_type = (
                AsyncConnection
                if managed_connector_with_data._driver_is_async
                else Connection
            )
            assert isinstance(connection, engine_type)

    async def test_reset_connections_sync_async_error(
        self, managed_connector_with_data
    ):
        with pytest.raises(RuntimeError, match="synchronous connections"):
            if managed_connector_with_data._driver_is_async:
                managed_connector_with_data.reset_connections()
            else:
                await managed_connector_with_data.reset_async_connections()

    async def test_fetch_one(self, managed_connector_with_data):
        results = await managed_connector_with_data.fetch_one("SELECT * FROM customers")
        assert results == ("Marvin", "Highway 42")
        results = await managed_connector_with_data.fetch_one("SELECT * FROM customers")
        assert results == ("Ford", "Highway 42")

        # test with parameters
        results = await managed_connector_with_data.fetch_one(
            "SELECT * FROM customers WHERE address = :address",
            parameters={"address": "Myway 88"},
        )
        assert results == ("Me", "Myway 88")
        assert len(managed_connector_with_data._unique_results) == 2

        # now reset so fetch starts at the first value again
        if managed_connector_with_data._driver_is_async:
            await managed_connector_with_data.reset_async_connections()
        else:
            managed_connector_with_data.reset_connections()
        assert len(managed_connector_with_data._unique_results) == 0

        # ensure it's really reset
        results = await managed_connector_with_data.fetch_one("SELECT * FROM customers")
        assert results == ("Marvin", "Highway 42")
        assert len(managed_connector_with_data._unique_results) == 1

    @pytest.mark.parametrize("size", [None, 1, 2])
    async def test_fetch_many(self, managed_connector_with_data, size):
        results = await managed_connector_with_data.fetch_many(
            "SELECT * FROM customers", size=size
        )
        expected = [("Marvin", "Highway 42"), ("Ford", "Highway 42")][
            : (size or managed_connector_with_data.fetch_size)
        ]
        assert results == expected

        # test with parameters
        results = await managed_connector_with_data.fetch_many(
            "SELECT * FROM customers WHERE address = :address",
            parameters={"address": "Myway 88"},
        )
        assert results == [("Me", "Myway 88")]
        assert len(managed_connector_with_data._unique_results) == 2

        # now reset so fetch starts at the first value again
        if managed_connector_with_data._driver_is_async:
            await managed_connector_with_data.reset_async_connections()
        else:
            managed_connector_with_data.reset_connections()
        assert len(managed_connector_with_data._unique_results) == 0

        # ensure it's really reset
        results = await managed_connector_with_data.fetch_many(
            "SELECT * FROM customers", size=3
        )
        assert results == [
            ("Marvin", "Highway 42"),
            ("Ford", "Highway 42"),
            ("Unknown", "Space"),
        ]
        assert len(managed_connector_with_data._unique_results) == 1

    async def test_fetch_all(self, managed_connector_with_data):
        # test with parameters
        results = await managed_connector_with_data.fetch_all(
            "SELECT * FROM customers WHERE address = :address",
            parameters={"address": "Highway 42"},
        )
        expected = [("Marvin", "Highway 42"), ("Ford", "Highway 42")]
        assert results == expected

        # there should be no more results
        results = await managed_connector_with_data.fetch_all(
            "SELECT * FROM customers WHERE address = :address",
            parameters={"address": "Highway 42"},
        )
        assert results == []
        assert len(managed_connector_with_data._unique_results) == 1

        # now reset so fetch one starts at the first value again
        if managed_connector_with_data._driver_is_async:
            await managed_connector_with_data.reset_async_connections()
        else:
            managed_connector_with_data.reset_connections()
        assert len(managed_connector_with_data._unique_results) == 0

        # ensure it's really reset
        results = await managed_connector_with_data.fetch_all(
            "SELECT * FROM customers WHERE address = :address",
            parameters={"address": "Highway 42"},
        )
        expected = [("Marvin", "Highway 42"), ("Ford", "Highway 42")]
        assert results == expected
        assert len(managed_connector_with_data._unique_results) == 1

    async def test_pickleable(self, managed_connector_with_data):
        await managed_connector_with_data.fetch_one("SELECT * FROM customers")
        pkl = cloudpickle.dumps(managed_connector_with_data)
        assert pkl
        assert cloudpickle.loads(pkl)

    async def test_close(self, managed_connector_with_data):
        if managed_connector_with_data._driver_is_async:
            with pytest.raises(RuntimeError, match="Please use the"):
                managed_connector_with_data.close()
        else:
            managed_connector_with_data.close()  # test calling it twice

    async def test_aclose(self, managed_connector_with_data):
        if not managed_connector_with_data._driver_is_async:
            with pytest.raises(RuntimeError, match="Please use the"):
                await managed_connector_with_data.aclose()
        else:
            await managed_connector_with_data.aclose()  # test calling it twice

    async def test_enter(self, managed_connector_with_data):
        if managed_connector_with_data._driver_is_async:
            with pytest.raises(RuntimeError, match="cannot be run"):
                with managed_connector_with_data:
                    pass

    async def test_aenter(self, managed_connector_with_data):
        if not managed_connector_with_data._driver_is_async:
            with pytest.raises(RuntimeError, match="cannot be run"):
                async with managed_connector_with_data:
                    pass
