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

Fixed registration error by changing `url` type to `AnyUrl` - [#29](https://github.com/PrefectHQ/prefect-sqlalchemy/pull/29)

### Security

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
