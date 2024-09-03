
import click

from ..renderer import Renderer
from ..auth import login_required
from .dpm import describe_group
from .client import DeployManagerClient


@describe_group.command(name='user', help='List DPM Users')
@click.pass_context
@login_required
def describe_user(ctx: click.Context):
    client = DeployManagerClient(ctx)
    fields = {
        'Username': '$.username',
        'Email': '$.email',
        'Current Org': '$.current_org.name',
    }
    users = client.get_users()
    click.echo(Renderer(data=users, fields=fields).render(output_format='plain'))
            