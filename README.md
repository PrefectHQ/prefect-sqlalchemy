# prefect-sqlalchemy

## Welcome!

Prefect integrations for interacting with SQLAlchemy.

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

### Write and run a flow

```python
from prefect import flow
from prefect_sqlalchemy.tasks import (
    goodbye_prefect_sqlalchemy,
    hello_prefect_sqlalchemy,
)


@flow
def example_flow():
    hello_prefect_sqlalchemy
    goodbye_prefect_sqlalchemy

example_flow()
```

## Resources

If you encounter any bugs while using `prefect-sqlalchemy`, feel free to open an issue in the [prefect-sqlalchemy](https://github.com/PrefectHQ/prefect-sqlalchemy) repository.

If you have any questions or issues while using `prefect-sqlalchemy`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-sqlalchemy` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-sqlalchemy.git

cd prefect-sqlalchemy/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
