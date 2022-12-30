# prefect-sqlalchemy

## Welcome!

Prefect integrations for interacting with various databases.

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-sqlalchemy/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-sqlalchemy?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/PrefectHQ/prefect-sqlalchemy/" alt="Stars">
        <img src="https://img.shields.io/github/stars/PrefectHQ/prefect-sqlalchemy?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-sqlalchemy/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-sqlalchemy?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/PrefectHQ/prefect-sqlalchemy/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/PrefectHQ/prefect-sqlalchemy?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-sqlalchemy` with `pip`:

```bash
pip install prefect-sqlalchemy
```

Then, register to [view the block](https://orion-docs.prefect.io/ui/blocks/) on Prefect Cloud:

```bash
prefect block register -m prefect_sqlalchemy
```

Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).

### Write and run a flow

#### Using a SyncDriver with SqlAlchemyConnector
Use `SqlAlchemyConnector` as a context manager to `execute` and `execute_many` operations; then, `fetch_many` and `fetch_one` operations.

```python
from prefect_sqlalchemy import SqlAlchemyConnector, SyncDriver, ConnectionComponents

with SqlAlchemyConnector(
    connection_info=ConnectionComponents(
        driver=SyncDriver.SQLITE_PYSQLITE,
        database="my.db"
    ),
) as database_credentials:
    database_credentials.execute(
        "CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);"
    )
    database_credentials.execute(
        "INSERT INTO customers (name, address) VALUES (:name, :address);",
        parameters={"name": "Marvin", "address": "Highway 42"},
    )
    database_credentials.execute_many(
        "INSERT INTO customers (name, address) VALUES (:name, :address);",
        seq_of_parameters=[
            {"name": "Ford", "address": "Highway 42"},
            {"name": "Unknown", "address": "Highway 42"},
        ],
    )
    # Repeated fetch* calls using the same operation will skip re-executing and instead return the next set of results
    print(database_credentials.fetch_many("SELECT * FROM customers", size=2))
    print(database_credentials.fetch_one("SELECT * FROM customers"))
```

#### Using an AsyncDriver with SqlAlchemyConnector

Use `SqlAlchemyConnector` as an async context manager to `execute` and `execute_many` operations; then, `fetch_many` and `fetch_one` operations.

```python
from prefect_sqlalchemy import SqlAlchemyConnector, AsyncDriver, ConnectionComponents

async with SqlAlchemyConnector(
    connection_info=ConnectionComponents(
        driver=AsyncDriver.SQLITE_AIOSQLITE,
        database="test.db"
    ),
) as database_credentials:
    await database_credentials.execute(
        "CREATE TABLE IF NOT EXISTS customers (name varchar, address varchar);"
    )
    await database_credentials.execute(
        "INSERT INTO customers (name, address) VALUES (:name, :address);",
        parameters={"name": "Marvin", "address": "Highway 42"},
    )
    await database_credentials.execute_many(
        "INSERT INTO customers (name, address) VALUES (:name, :address);",
        seq_of_parameters=[
            {"name": "Ford", "address": "Highway 42"},
            {"name": "Unknown", "address": "Highway 42"},
        ],
    )
    # Repeated fetch* calls using the same operation will skip re-executing and instead return the next set of results
    print(await database_credentials.fetch_many("SELECT * FROM customers", size=2))
    print(await database_credentials.fetch_one("SELECT * FROM customers"))
```

## Resources

If you encounter any bugs while using `prefect-sqlalchemy`, feel free to open an issue in the [prefect-sqlalchemy](https://github.com/PrefectHQ/prefect-sqlalchemy) repository.

If you have any questions or issues while using `prefect-sqlalchemy`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to ⭐️ or watch [`prefect-sqlalchemy`](https://github.com/PrefectHQ/prefect-sqlalchemy) for updates too!

## Development

If you'd like to install a version of `prefect-sqlalchemy` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-sqlalchemy.git

cd prefect-sqlalchemy/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
