from os import linesep

import click

from ..renderer import Renderer, output_format_option, RenderField
from ..auth import login_required
from .dpm import list_group, describe_group, update_group
from .client import DeployManagerClient

@update_group.group(name='route', help='Update DPM Routes')
def update_route():
    pass

@list_group.command(name='routes', help='List DPM Routes')
@click.pass_context
@click.option('--search', help='Search by route name, project_name or project description', 
              default=None, type=str)
@click.option('--org', help='Organization name', default=None, type=str)
@click.option('--user', help='Route owner email', default=None, type=str)
@click.option('--status', help='Route status', default=None, type=str)
@click.option('--project', help='Project name', default=None, type=str)
@click.option('--stage', help='Stage name', default=None, type=str)
@click.option('--open', help='Show only open-access routes', default=None, type=bool, is_flag=True)
@click.option('--apps', help='Show only App routes', default=None, type=bool, is_flag=True)
@click.option('--services', help='Show only Service routes', default=None, type=bool, is_flag=True)
@output_format_option
@login_required
def list_routes(ctx: click.Context, output: str, open: bool, services: bool, apps: bool, **filters):
    if apps:
        filters.update({'publish_app': True})
    if services:
        filters.update({'publish_app': False})
    if open:
        filters.update({'open_access': True})

    def format_status(status: str) -> str:
        if status == 'online':
            return click.style(status.upper(), fg='green')
        elif status == 'offline':
            return click.style(status.upper(), fg='black')
        elif status == 'pending':
            return click.style(status.upper(), fg='yellow')
        elif status == 'starting':
            return click.style(status.upper(), fg='blue')
        elif status == 'error':
            return click.style(status.upper(), fg='red', bold=True)
        else:
            return status

    client = DeployManagerClient(ctx)
    fields = [
        RenderField(label='Name', path='$.name'),
        RenderField(label='Project', path='$.project'),
        RenderField(label='Stage', path='$.stage'),
        RenderField(label='Status', path='$.status', mod=format_status),
        RenderField(label='URL', path='$.url'),
    ]
    routes =  client.list_routes(**{
        k: v for k, v in filters.items() if v is not None
    })
    if not routes:
        click.echo('No routes found!')
    else:
        click.echo(Renderer(data=routes, fields=fields).render(output_format=output))

@describe_group.command(name='route', help='Describe a DPM Service or App Route')
@click.pass_context
@click.argument('route_name', type=str)
@login_required
def describe_route(ctx: click.Context, route_name: str):
    client = DeployManagerClient(ctx)
    route = client.get_route(route_name)
    fields = [
        RenderField(label='Name', path='$.name'),
        RenderField(label='Project', path='$.project'),
        RenderField(label='Service', path='$.org'),
        RenderField(label='Stage', path='$.stage'),
        RenderField(label='Org', path='$.org'),
        RenderField(label='Owner', path='$.username'),
        RenderField(label='Status', path='$.status'),
        RenderField(label='Default URL', path='$.url'),
        RenderField(
            label='Custom Domains', 
            path='$.custom_domains.*', 
            sep=linesep, 
            mod=lambda x: f'https://{x}/' if x else None
        ),
        RenderField(label='Publish App', path='$.publish_app'),
        RenderField(label='Open Access', path='$.open_access'),
        RenderField(label='Thumbnail URL', path='$.thumbnail'),
    ]
        
    if route is not None:
        click.echo(Renderer(data=[route], fields=fields).render(output_format='plain'))
    #     click.echo()
    #     click.echo(f"Describing route '{route_name}'...")
    #     click.echo()
    #     output = [
    #         ['Name', route.name],
    #         ['Project', route.project],
    #         ['Stage', route.stage],
    #         ['Status', route.status],
    #         ['URL', route.url],
    #     ]
    #     if route.custom_domains:
    #         output.append(['Custom Domains', os.linesep.join(route.custom_domains)])
    #     if route.publish_app is not None:
    #         output.append(['Publish App', route.publish_app])
    #     if route.open_access is not None:
    #         output.append(['Open Access', route.open_access])
        #route_dict = route.model_dump(mode='json', by_alias=True, exclude_none=True, exclude_unset=True)
#        click.echo(yaml.dump(route_dict))
    else:
        click.echo(f"Route '{route_name}' not found!")


@update_route.command(name='thumbnail', help='Update a DPM Route thumbnail')
@click.pass_context
@click.argument('route_name', type=str)
@click.argument('thumbnail_file', type=click.File('rb'))
@login_required
def update_thumbnail(ctx: click.Context, route_name: str, thumbnail_file: click.File):
    client = DeployManagerClient(ctx)
    route = client.get_route(route_name)
    if route is not None:
        click.echo(f"Updating thumbnail for route '{route_name}'...")
        try:
            thumbnail = client.update_route_thumbnail(route_name, thumbnail_file)
            click.echo(f"Thumbnail updated successfully for route '{route_name}'!")
        except Exception as e:
            click.echo(f"ERROR: Failed to update thumbnail for route '{route_name}': {e}")
            raise
    else:
        click.echo(f"Route '{route_name}' not found!")
