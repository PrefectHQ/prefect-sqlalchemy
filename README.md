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

A list of available blocks in `prefect-sqlalchemy` and their setup instructions can be found [here](https://PrefectHQ.github.io/prefect-sqlalchemy/#blocks-catalog).

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

Use `with_options` to customize options on any existing task or flow

```python
custom_sqlalchemy_execute_flow = sqlalchemy_execute_flow.with_options(
    name="My custom flow name",
    retries=2,
    retry_delay_seconds=10,
)
```

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://orion-docs.prefect.io/collections/usage/)!

## Resources

If you encounter any bugs while using `prefect-sqlalchemy`, feel free to open an issue in the [prefect-sqlalchemy](https://github.com/PrefectHQ/prefect-sqlalchemy) repository.

If you have any questions or issues while using `prefect-sqlalchemy`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to star or watch [`prefect-sqlalchemy`](https://github.com/PrefectHQ/prefect-sqlalchemy) for updates too!

## Contribute

If you'd like to help contribute to fix an issue or add a feature to `prefect-sqlalchemy`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

### Contribution Steps:
1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes.
5. Add tests.
6. Insert an entry to [CHANGELOG.md](https://github.com/PrefectHQ/prefect-sqlalchemy/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request.
