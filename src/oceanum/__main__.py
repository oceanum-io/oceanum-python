import sys

import click

# When adding a new module, must be imported here

from importlib.metadata import entry_points

from oceanum.cli.main import main
from oceanum.cli import auth, datamesh, storage

run_eps = entry_points(group='oceanum.cli.run.main')
try:
    run_eps['run'].load()
except ModuleNotFoundError:
    run = None
    click.echo('Oceanum Run module not found, please install oceanum-run-cli package')


