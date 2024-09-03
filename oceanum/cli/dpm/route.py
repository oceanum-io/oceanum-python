
import click

from ..renderer import Renderer, output_format_option
from ..auth import login_required
from .dpm import list_group, describe_group
from .client import DeployManagerClient

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

    client = DeployManagerClient(ctx)
    fields = {
        'Name': '$.name',
        'Project' : '$.project',
        'Stage': '$.stage',
        'Status': '$.status',
        'URL': '$.url',
    }
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
    
    fields = {
        'Name': '$.name',
        'Project': '$.project',
        'Stage': '$.stage',
        'Org': '$.org',
        'Owner': '$.username',
        'Status': '$.status',
        'Default URL': '$.url',
        'Custom Domains': '$.custom_domains',
        'Publish App': '$.publish_app',
        'Open Access': '$.open_access',
        'Thumbnail': '$.thumbnail',
    }
        
    if route is not None:
        click.echo(Renderer(data=route, fields=fields).render(output_format='plain'))
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