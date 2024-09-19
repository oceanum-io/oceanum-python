import sys

import click

from .cli.storage import storage

# When adding a new module, must be imported here
from .cli import main, auth, datamesh, storage

try:
    main.main()
except KeyboardInterrupt:
    sys.exit(1)
except Exception as e:
    click.echo(f'Error: {e}')
    raise e
    sys.exit(1)