# -*- coding: utf-8 -*-

"""Console script for oceanum library."""
import sys
import click


@click.command()
def main(args=None):
    """Console interface for oceanum."""
    click.echo("Oceanum.io CLI. Just a stub at present.")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
