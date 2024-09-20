import sys

import click

# When adding a new module, must be imported here

from importlib.metadata import entry_points

from oceanum.cli.main import main
from oceanum.cli import auth, datamesh, storage

try:
    for run_ep in entry_points(group='oceanum.cli.run.main'):
        run_ep.load()
except ModuleNotFoundError as e:
    click.echo(f'Oceanum Run module {e} not found! Please install oceanum-run-cli package')


