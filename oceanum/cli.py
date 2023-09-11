"""Console script for oceanum library."""
import click

from oceanum.storage import filesystem


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
    contents = filesystem.ls(
        path, recursive, long, credentials.datamesh_token, credentials.storage_service,
    )
    if long:
        sizes = 0
        for item in contents.values():
            line, size = item_to_long(item, human_readable=human_readable)
            sizes += size
            click.echo(line)
        click.echo(
            f"TOTAL: {len(contents)} objects, {sizes} bytes ({bytes_to_human(sizes)})"
        )
    else:
        click.echo("\n".join(contents))


@storage.command()
@click.option("-r", "--recursive", is_flag=True, help="Copy directories recursively")
@click.argument("source")
@click.argument("dest")
@pass_credentials
def get(
    credentials: Credentials,
    recursive: bool,
    source: str,
    dest: str,
):
    """Copy content from SOURCE to DEST."""
    filesystem.get(
        source=source,
        dest=dest,
        recursive=recursive,
        token=credentials.datamesh_token,
        service=credentials.storage_service,
    )


@storage.command()
@click.option("-r", "--recursive", is_flag=True, help="Copy directories recursively")
@click.argument("source")
@click.argument("dest")
@pass_credentials
def put(
    credentials: Credentials,
    recursive: bool,
    source: str,
    dest: str,
):
    """Copy content from SOURCE to DEST."""
    filesystem.put(
        source=source,
        dest=dest,
        recursive=recursive,
        token=credentials.datamesh_token,
        service=credentials.storage_service,
    )


@storage.command()
@click.option("-r", "--recursive", is_flag=True, help="Remove directories recursively")
@click.argument("path")
@pass_credentials
def rm(credentials: Credentials, recursive: bool, path: str):
    """Remove PATH."""
    filesystem.rm(
        path=path,
        recursive=recursive,
        token=credentials.datamesh_token,
        service=credentials.storage_service,
    )

# =====================================================================================
# Datamesh CLI
# =====================================================================================
@main.group()
def datamesh():
    """Oceanum datamesh commands."""
    pass
