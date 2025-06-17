
import click

from ...storage import filesystem

from ..main import main
from ..common.utils import bytes_to_human, item_to_long
from ..auth import login_required

# =====================================================================================
# Storage CLI
# =====================================================================================
@main.group()
def storage():
    """Oceanum Storage commands"""
    click.help_option('-h', '--help')

@storage.command()
@click.option("-l", "--long", is_flag=True, help="Long listing format")
@click.option("-h", "--human-readable", is_flag=True, help="Readable sizes with -l")
@click.option("-r", "--recursive", is_flag=True, help="List subdirectories recursively")
@click.argument("path", default="/")
@click.pass_context
@login_required
def ls(
    ctx: click.Context,
    path: str,
    long: bool,
    human_readable: bool,
    recursive: bool,
):
    """List contents in the oceanum storage (the root directory by default)."""
    contents = filesystem.ls(
        path=path,
        recursive=recursive,
        detail=long,
        token=f"Bearer {ctx.obj.token.access_token}",
        service=f"https://storage.{ctx.obj.domain}/",
    )
    if long:
        sizes = 0
        for item in contents.values(): # type: ignore
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
@click.pass_context
def get(
    ctx: click.Context,
    recursive: bool,
    source: str,
    dest: str,
):
    """Copy content from SOURCE to DEST."""
    filesystem.get(
        source=source,
        dest=dest,
        recursive=recursive,
        token=f"Bearer {ctx.obj.token.access_token}",
        service=f"https://storage.{ctx.obj.domain}/",
    )


@storage.command()
@click.option("-r", "--recursive", is_flag=True, help="Copy directories recursively")
@click.argument("source")
@click.argument("dest")
@click.pass_context
#
def put(
    ctx: click.Context,
    recursive: bool,
    source: str,
    dest: str,
):
    """Copy content from SOURCE to DEST."""
    filesystem.put(
        source=source,
        dest=dest,
        recursive=recursive,
        token=f"Bearer {ctx.obj.token.access_token}",
        service=f"https://storage.{ctx.obj.domain}/",
    )


@storage.command()
@click.option("-r", "--recursive", is_flag=True, help="Remove directories recursively")
@click.argument("path")
@click.pass_context
def rm(ctx: click.Context, recursive: bool, path: str):
    """Remove PATH."""
    filesystem.rm(
        path=path,
        recursive=recursive,
        token=f"Bearer {ctx.obj.token.access_token}",
        service=f"https://storage.{ctx.obj.domain}/",
    )
