"""  Datamesh CLI commands """

import click

from ..main import main
from ..auth import login_required
from ..datamesh.connection import Connector

@main.group(help='Datamesh Datasources commands')
def datamesh():
    pass

@datamesh.group(name='list')
def list_():
    pass

class DatameshClient:
    def __init__(self, ctx: click.Context) -> None:
        service_url = f'https://datamesh.{ctx.obj.domain}/'
        self.connector = Connector(
            token= f'Bearer {ctx.obj.token.access_token}', 
            service=service_url
        )

    def __enter__(self):
        return self.connector

    def __exit__(self, exc_type, exc_value, traceback):
        pass

@list_.command()
@click.pass_context
@click.option('--search', help='Search string', default=None, type=str)
@click.option('--limit', help='Limit results', default=10)
@login_required
def datasources(ctx: click.Context, search: str, limit: int):
    with DatameshClient(ctx) as client:
        datasources = client.get_catalog(search=search, limit=limit)
        click.echo(datasources)

