"""Console script for oceanum library."""
import click
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


class Auth:
    def __init__(self, token=None):
        self.token = token


pass_auth = click.make_pass_decorator(Auth, ensure=True)


@click.group()
@click.option(
    "-t",
    "--token",
    help="Datamesh token, env DATAMESH_TOKEN by default",
    envvar="DATAMESH_TOKEN",
)
@pass_auth
def main(auth, token):
    """Oceanum python commands."""
    auth.token = token


# =====================================================================================
# Storage CLI
# =====================================================================================
@main.group()
def storage():
    """Oceanum storage commands."""
    pass


@storage.command()
@click.option("-l", "--long", is_flag=True, help="Long listing format")
@click.option("-h", "--human-readable", is_flag=True, help="Readable sizes with -l")
@click.option("-r", "--recursive", is_flag=True, help="List subdirectories recursively")
@click.argument("path", default="/")
@pass_auth
def ls(auth, path, long, human_readable, recursive):
    """List contents in the oceanum storage (the root directory by default)."""
    fs = FileSystem(auth.token)
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
@pass_auth
def cp(auth, source, dest, recursive, token):
    """Copy SOURCE to DEST."""
    pass


# =====================================================================================
# Datamesh CLI
# =====================================================================================
@main.group()
def datamesh():
    """Oceanum datamesh commands."""
    pass

