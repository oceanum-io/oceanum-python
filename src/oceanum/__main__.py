import os
import sys
import types
import click
from importlib.metadata import entry_points
from oceanum.cli import main as cli_main

CLI_DEBUG = bool(os.getenv('OCEANUM_CLI_DEBUG'))


def load_cli_extensions(parent_group=None):
    # Load CLI extension entry points at call time to avoid side effects on import
    if parent_group is None:
        parent_group = cli_main
    for run_ep in entry_points(group='oceanum.cli.extensions'):
        try:
            if CLI_DEBUG:
                print(f"Loading entry point: {run_ep.name}...")
            ep_obj = run_ep.load()
            # Module object: legacy behaviour (side effects only)
            if isinstance(ep_obj, types.ModuleType):
                if CLI_DEBUG:
                    print(f"Loaded module '{getattr(ep_obj, '__name__', run_ep.name)}' successfully.")
            # Command/Group exported directly
            elif isinstance(ep_obj, click.Command):
                name = getattr(ep_obj, 'name', None) or run_ep.name
                parent_group.add_command(ep_obj, name=name)
                if CLI_DEBUG:
                    print(f"Registered command '{name}' from entry point '{run_ep.name}'.")
            # Registrar callable
            elif callable(ep_obj):
                ep_obj(parent_group)
                if CLI_DEBUG:
                    print(f"Called registrar from entry point '{run_ep.name}'.")
            else:
                if CLI_DEBUG:
                    print(f"Unknown entry point target type for '{run_ep.name}': {type(ep_obj)}", file=sys.stderr)
        except Exception as e:
            print(f"Error loading entry point {run_ep.name}: {e}", file=sys.stderr)


def main():
    load_cli_extensions(cli_main)
    cli_main()

if __name__ == "__main__":
    main()
