import sys

import click

# When adding a new module, must be imported here

from importlib.metadata import entry_points

from oceanum.cli.main import main
from oceanum.cli import auth, datamesh, storage

for cli_ep in entry_points(group="oceanum.cli"):
    try:
        cli_ep.load()
    except ModuleNotFoundError as e:
        pass
