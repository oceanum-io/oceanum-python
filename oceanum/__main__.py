import sys

import click

from .cli.main import main

try:
    main()
except KeyboardInterrupt:
    sys.exit(1)
except Exception as e:
    click.echo(f'Error: {e}')
    sys.exit(1)