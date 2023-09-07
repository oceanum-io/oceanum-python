"""Console script for oceanum library."""
import click
from pathlib import Path
from aiohttp import ClientResponseError

from oceanum.storage import FileSystem


def bytes_to_human(size):
    """Convert bytes to human-readable format."""
    if size < 1024:
        return f"{size} B"
    elif size < 1024 ** 2:
        return f"{size / 1024:.1f} KiB"
    elif size < 1024 ** 3:
        return f"{size / 1024 ** 2:.1f} MiB"
    elif size < 1024 ** 4:
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
# Storage CLI
# =====================================================================================
@main.group()
@click.option(
    "-s",
    "--service",
    default="https://storage.oceanum.io",
    help="Storage service, defaults to STORAGE_SERVICE env or https://storage.oceanum.io",
    envvar="STORAGE_SERVICE",
)
@pass_credentials
def storage(credentials: Credentials, service: str):
    """Oceanum storage commands."""
    credentials.storage_service = service


@storage.command()
@click.option("-l", "--long", is_flag=True, help="Long listing format")
@click.option("-h", "--human-readable", is_flag=True, help="Readable sizes with -l")
@click.option("-r", "--recursive", is_flag=True, help="List subdirectories recursively")
@click.argument("path", default="/")
@pass_credentials
def ls(
    credentials: Credentials,
    path: str,
    long: bool,
    human_readable: bool,
    recursive: bool,
):
    """List contents in the oceanum storage (the root directory by default)."""
    fs = FileSystem(
        token=credentials.datamesh_token, service=credentials.storage_service
    )
    try:
        maxdepth = None if recursive else 1
        items = fs.find(path, maxdepth=maxdepth, withdirs=True, detail=long)
        if long:
            sizes = 0
            for item in items.values():
                line, size = item_to_long(item, human_readable=human_readable)
                sizes += size
                click.echo(line)
            click.echo(
                f"TOTAL: {len(items)} objects, {sizes} bytes ({bytes_to_human(sizes)})"
            )
        else:
            click.echo("\n".join(items))
    except ClientResponseError:
        click.echo(f"Path {path} not found or not authorised (check datamesh token)")


@storage.command()
@click.argument("source")
@click.argument("dest")
@click.option("-r", "--recursive")
@click.option("-o", "--overwrite", is_flag=True, help="Overwrite existing destination")
@pass_credentials
def get(
    credentials: Credentials,
    source: str,
    dest: str,
    recursive: bool,
    overwrite: bool,
):
    """Copy content from SOURCE to DEST."""
    fs = FileSystem(
        token=credentials.datamesh_token, service=credentials.storage_service
    )
    # checking
    dest = Path(dest)
    is_source_dir = fs.isdir(source)
    if not overwrite and dest.is_file():
        raise FileExistsError(
            f"Destination {dest} already exists, set --overwrite to overwrite it"
        )
    if not fs.isfile(source) and not is_source_dir:
        raise FileNotFoundError(f"Source {source} not found")
    if is_source_dir and not recursive:
        raise IsADirectoryError(f"--recursive is required to get directory {source}")
    if dest.is_dir() and Path(source).name != dest.name:
        dest = str(dest / Path(source).name)
    # Downloading
    fs.get(source, str(dest), recursive=recursive)


# =====================================================================================
# Datamesh CLI
# =====================================================================================
@main.group()
def datamesh():
    """Oceanum datamesh commands."""
    pass
