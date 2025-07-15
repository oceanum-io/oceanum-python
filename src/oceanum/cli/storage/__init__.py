
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
@click.option("-f", "--force", is_flag=True, help="Force removal without confirmation")
@click.argument("path")
@click.pass_context
@login_required
def rm(ctx: click.Context, recursive: bool, force: bool, path: str):
    """Remove PATH."""
    if not force:
        if recursive:
            message = f"Are you sure you want to recursively remove '{path}' and all its contents?"
        else:
            message = f"Are you sure you want to remove '{path}'?"

        if not click.confirm(message):
            click.echo("Operation cancelled.")
            return

    try:
        filesystem.rm(
            path=path,
            recursive=recursive,
            token=f"Bearer {ctx.obj.token.access_token}",
            service=f"https://storage.{ctx.obj.domain}/",
        )
        click.echo(f"Successfully removed: {path}")
    except FileNotFoundError:
        click.echo(f"Error: Path '{path}' not found", err=True)
        ctx.exit(1)
    except OSError as e:
        if "Directory not empty" in str(e):
            click.echo(f"Error: Directory '{path}' is not empty. Use -r/--recursive to remove non-empty directories.", err=True)
        else:
            click.echo(f"Error: {e}", err=True)
        ctx.exit(1)
    except Exception as e:
        click.echo(f"Error removing '{path}': {e}", err=True)
        ctx.exit(1)


@storage.command()
@click.argument("path")
@click.pass_context
@login_required
def exists(ctx: click.Context, path: str):
    """Check if PATH exists in storage."""
    try:
        result = filesystem.exists(
            path=path,
            token=f"Bearer {ctx.obj.token.access_token}",
            service=f"https://storage.{ctx.obj.domain}/",
        )
        if result:
            click.echo(f"EXISTS: {path}")
        else:
            click.echo(f"NOT FOUND: {path}")
            ctx.exit(1)
    except Exception as e:
        click.echo(f"Error checking '{path}': {e}", err=True)
        ctx.exit(1)


@storage.command()
@click.argument("path")
@click.pass_context
@login_required
def isfile(ctx: click.Context, path: str):
    """Check if PATH is a file in storage."""
    try:
        result = filesystem.isfile(
            path=path,
            token=f"Bearer {ctx.obj.token.access_token}",
            service=f"https://storage.{ctx.obj.domain}/",
        )
        if result:
            click.echo(f"FILE: {path}")
        else:
            click.echo(f"NOT A FILE: {path}")
            ctx.exit(1)
    except Exception as e:
        click.echo(f"Error checking '{path}': {e}", err=True)
        ctx.exit(1)


@storage.command()
@click.argument("path")
@click.pass_context
@login_required
def isdir(ctx: click.Context, path: str):
    """Check if PATH is a directory in storage."""
    try:
        result = filesystem.isdir(
            path=path,
            token=f"Bearer {ctx.obj.token.access_token}",
            service=f"https://storage.{ctx.obj.domain}/",
        )
        if result:
            click.echo(f"DIRECTORY: {path}")
        else:
            click.echo(f"NOT A DIRECTORY: {path}")
            ctx.exit(1)
    except Exception as e:
        click.echo(f"Error checking '{path}': {e}", err=True)
        ctx.exit(1)
