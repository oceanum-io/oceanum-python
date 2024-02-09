"""Console script for oceanum library."""

import click

from oceanum.storage import filesystem


def bytes_to_human(size):
    """Convert bytes to human-readable format."""
    if size < 1024:
        return f"{size} B"
    elif size < 1024**2:
        return f"{size / 1024:.1f} KiB"
    elif size < 1024**3:
        return f"{size / 1024 ** 2:.1f} MiB"
    elif size < 1024**4:
        return f"{size / 1024 ** 3:.1f} GiB"
    else:
        return f"{size / 1024 ** 4:.1f} TiB"


def item_to_long(item, human_readable=False):
    """Convert item to long listing format."""
    size = item["size"]
    if human_readable:
        size = bytes_to_human(size)
    elif size == 0:
        size = ""
    modified = item["modified"] or ""
    return f"{size:>10}  {modified:>32}  {item['name']}", item["size"]


class Credentials:
    def __init__(
        self, datamesh_token=None, storage_service="https://storage.oceanum.io"
    ):
        self.datamesh_token = datamesh_token
        self.storage_service = storage_service


pass_credentials = click.make_pass_decorator(Credentials, ensure=True)


@click.group()
@click.option(
    "-t",
    "--token",
    help="Datamesh token, defaults to DATAMESH_TOKEN env",
    envvar="DATAMESH_TOKEN",
)
@pass_credentials
def main(credentials, token):
    """Oceanum python commands."""
    credentials.datamesh_token = token


# =====================================================================================
# Datamesh CLI
# =====================================================================================
@main.group()
def datamesh():
    """Oceanum datamesh commands."""
    pass
