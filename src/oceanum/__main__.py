import sys

import click

from importlib.metadata import entry_points

from oceanum.cli.main import main
from oceanum.cli import auth, datamesh, storage

# Load CLI plugins from entry points
for cli_ep in entry_points(group='oceanum.cli'):
    try:
        plugin_module = cli_ep.load()
        # Look for the command with the same name as the entry point
        if hasattr(plugin_module, cli_ep.name):
            command = getattr(plugin_module, cli_ep.name)
            if hasattr(command, '__call__') and hasattr(command, 'name'):
                main.add_command(command, name=cli_ep.name)
        elif hasattr(plugin_module, 'cli'):
            main.add_command(plugin_module.cli, name=cli_ep.name)

        # For plugins like prax, also load submodules to register their commands
        if cli_ep.name == 'prax':
            # Load all prax submodules to register their commands
            for prax_ep in entry_points(group='oceanum.cli.prax'):
                try:
                    prax_ep.load()
                except (ModuleNotFoundError, ImportError, AttributeError):
                    # Submodule not available or incorrectly configured, skip silently
                    pass
    except (ModuleNotFoundError, ImportError, AttributeError) as e:
        # Plugin not available or incorrectly configured, skip silently
        pass

if __name__ == '__main__':
    main()
