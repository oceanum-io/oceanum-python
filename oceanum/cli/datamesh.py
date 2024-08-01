"""  Datamesh CLI commands """

import click
import httpx

from .base import get
from .auth import login_required
from ..datamesh.connection import Connector


class DatameshClient:
    def __init__(self, ctx: click.Context) -> None:
        service_url = f'https://datamesh.{ctx.obj.domain}/'
        self.connector = Connector(
            bearer=ctx.obj.token.access_token, 
            service=service_url
        )

    def __enter__(self):
        return self.connector

    def __exit__(self, exc_type, exc_value, traceback):
        pass

@get.command()
@click.pass_context
@click.option('--search', help='Search string', default=None)
@click.option('--limit', help='Limit results', default=10)
@login_required
def datasources(ctx: click.Context, search: str, limit: int):
    with DatameshClient(ctx) as client:
        datasources = client.get_catalog(search=search, limit=limit)
        print(datasources)

