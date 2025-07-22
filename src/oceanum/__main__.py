import os
import sys
import importlib
from importlib.metadata import entry_points
from oceanum.cli import main

CLI_DEBUG = bool(os.getenv('OCEANUM_CLI_DEBUG'))

for run_ep in entry_points(group='oceanum.cli.extensions'):
    try:
        if CLI_DEBUG:
            print(f"Loading entry point: {run_ep.name}...")
        ep_module = run_ep.load()  
        if CLI_DEBUG:
            print(f"Imported module '{ep_module.__name__}' successfully.")
    except ModuleNotFoundError as e:
        print(f"Error loading entry point {run_ep.name}: {e}", file=sys.stderr)
        pass

if __name__ == "__main__":
    main()