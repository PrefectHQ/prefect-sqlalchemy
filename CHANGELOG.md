# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## 0.2.4

Released on February 15th, 2022.

### Fixed

- Initializing `_unique_results` and `_exit_stack` so `fetch_*` is able to run - [#49](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/49)

## 0.2.3

Released on February 10th, 2022.

### Changed

- Delay start of engine until necessary - [#46](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/46)

### Fixed

- Using SQLite driver in sync context - [#45](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/45)

## 0.2.2

Released on December 30th, 2022.

### Added

- `SqlAlchemyConnector` database block - [#35](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/35)
- `ConnectionComponents` base model - [#35](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/35)

### Deprecated

- `DatabaseCredentials` and all tasks in `prefect_sqlalchemy.database`, in favor of `SqlAlchemyConnector` - [#35](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/35)

## 0.2.1

Released on December 15th, 2022.

- Serialization of `DatabaseCredentials.rendered_url` - [#36](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/36)

## 0.2.0

Released on September 1st, 2022.

### Changed

- `DatabaseCredentials` now only accepts a `str` in URL format - [#29](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/29)

### Fixed

- Fixed registration error by changing `url` type to `AnyUrl` - [#29](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/29)

## 0.1.2

Released on August 1st, 2022.

### Fixed

- Fixed `sqlite+*` drivers by dropping 'username' from the required input keywords - [#23](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/23)
- Fixed `sqlalchemy_query` for drivers that require the connection to be alive while fetching - [#23](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/23)

## 0.1.1

Released on July 26th, 2022.

### Changed

- Updated tests to be compatible with core Prefect library (v2.0b9) and bumped required version - [#18](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/18)
- Converted `DatabaseCredentials` into a `Block` - [#19](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/19)
- Improve docstring; clarifying `limit` in `sqlalchemy_execute` is executed on client side - [#21](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/21)

### Fixed
- Fixed `DatabaseCredentials` to `get_secret_value` for password - [#22](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/22)

## 0.1.0

Released on June 3rd, 2022.

### Added

- `sqlalchemy_execute` and `sqlalchemy_query` tasks - [#3](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/3)
- Support for synchronous and external drivers - [#5](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/5)
