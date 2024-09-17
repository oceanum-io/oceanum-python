
import click

from ..renderer import Renderer, RenderField
from ..auth import login_required
from .dpm import describe_group
from .client import DeployManagerClient



@describe_group.command(name='user', help='List DPM Users')
@click.pass_context
@login_required
def describe_user(ctx: click.Context):
    client = DeployManagerClient(ctx)
    fields = [
        RenderField(label='Username', path='$.username'),
        RenderField(label='Email', path='$.email'),
        RenderField(label='Current Org', path='$.current_org.name'),
        RenderField(label='DPM API Token', path='$.token'),
        RenderField(
            label='User Resources', 
            path='$.resources.*', 
            mod=lambda x: f"{x['resource_type'].removesuffix('s')}: {x['name']}"),
    ]
    users = client.get_users()
    click.echo(Renderer(data=users, fields=fields).render(output_format='plain'))