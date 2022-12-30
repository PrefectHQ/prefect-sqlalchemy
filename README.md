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

Create table named customers and insert values.
```python
from prefect import flow

from prefect_sqlalchemy import DatabaseCredentials, AsyncDriver
from prefect_sqlalchemy.database import sqlalchemy_execute


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
