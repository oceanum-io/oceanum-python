# oceanum-python

Python library for working with the [Oceanum.io platform](https://oceanum.io).

Documentation is at: (https://oceanum-python.readthedocs.io/)

## Datasource notices

When you load, query or get a datasource, the Datamesh `Connector` shows oceanum.io notices about it once per datasource per process: `outage` and `action_required` as a `NoticeWarning`, `change` and `info` as an `INFO` log line. `Connector.get_notices(datasource_id)` returns them as a list. The check never raises, but with no cached answer it can add up to 3 seconds to a call. Turn it off with `OCEANUM_NOTICES=0` or `Connector(notices=False)`. `OCEANUM_ADMIN_SERVICE` overrides the admin service url, and that service receives your credential. See the "Datasource Notices" section of the usage docs.

## Testing

The default test account has no datamesh write access. To test the write functionality, you will need to replace the DATAMESH_TOKEN in tox.ini with a token from an account with write access.

Unit tests and storage tests can be run via pytest. Tests requiring valid credentials are marked with `requires_datamesh_token` and expect the `OCEANUM_TEST_DATAMESH_TOKEN` environment variable to be set.

## Plugin Authoring

The `oceanum` CLI can be extended via entry points. To add a new command or group, register it in your `pyproject.toml` under the `[project.entry-points."oceanum.cli.extensions"]` group.

Supported extension targets:
- **Module**: Importing the module registers commands via side effects (legacy).
- **Direct Click Object**: A `click.Command` or `click.Group` object.
- **Registrar Callable**: A function with signature `register_cli(parent_group: click.Group)`.
