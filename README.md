# oceanum-python

Python library for working with the [Oceanum.io platform](https://oceanum.io).

Documentation is at: (https://oceanum-python.readthedocs.io/)

## Testing

The default test account has no datamesh write access. To test the write functionality, you will need to replace the DATAMESH_TOKEN in tox.ini with a token from an account with write access.

Unit tests and storage tests can be run via pytest. Tests requiring valid credentials are marked with `requires_datamesh_token` and expect the `OCEANUM_TEST_DATAMESH_TOKEN` environment variable to be set.

## Plugin Authoring

The `oceanum` CLI can be extended via entry points. To add a new command or group, register it in your `pyproject.toml` under the `[project.entry-points."oceanum.cli.extensions"]` group.

Supported extension targets:
- **Module**: Importing the module registers commands via side effects (legacy).
- **Direct Click Object**: A `click.Command` or `click.Group` object.
- **Registrar Callable**: A function with signature `register_cli(parent_group: click.Group)`.
