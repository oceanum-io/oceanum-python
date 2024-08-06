
import click
import os
import httpx
from pathlib import Path
import pprint

from tabulate import tabulate

from .main import main
from .auth import login_required
from .models import TokenResponse
from ..dpm.client import DPMHttpClient
from ..dpm.models import UserRef

class DpmContextedClient:
    def __init__(self, ctx: click.Context) -> None:
        self.ctx = ctx

    def __enter__(self):
        if 'oceanum.' in self.ctx.obj.domain:
            service_url = f'https://dpm.{self.ctx.obj.domain}/api/'
        else:
            # Allow for local development
            service_url = f'http://{self.ctx.obj.domain}/'
        self.client = DPMHttpClient(
            token=f"Bearer {self.ctx.obj.token.access_token}",
            service=service_url
        )
        return self.client
    def __exit__(self, exc_type, exc_value, traceback):
        pass

@main.group()
def dpm():
    pass

@dpm.group()
def get():
    pass

@dpm.group()
def describe():
    pass

@dpm.command()
@click.argument('specfile', type=click.Path(exists=True))
@click.pass_context
@login_required
def validate(ctx: click.Context, specfile: click.Path):
    click.echo('Validating DPM Project Spec...')
    with DpmContextedClient(ctx) as client:
        client.validate(Path(str(specfile)))
    click.echo('Project spec is valid!')

@dpm.command()
@click.option('--name', help='Overwrite project name from suplied specfile', required=False, type=str)
@click.option('--org', help='Overwrite organization name', required=False, type=str)
@click.option('--user', help='Overwrite user email', required=False, type=str)
@click.option('--wait', help='Wait for project to be deployed', is_flag=True)
@click.argument('specfile', type=click.Path(exists=True))
@click.pass_context
@login_required
def deploy(
    ctx: click.Context, 
    specfile: click.Path, 
    name: str|None, 
    org: str|None, 
    user: str|None
):
    click.echo('Creating DPM Project...')
    with DpmContextedClient(ctx) as client:
        project_spec = client.load_spec(Path(str(specfile)))
        if name:
            project_spec.name = name
        if org is not None:
            project_spec.user_ref = UserRef(org)
        if user:
            project_spec.member_ref = user
        project = client.deploy_project(project_spec)
    click.echo(f'Project created or updated successfully: {project.name}')

@get.command()
@click.pass_context
@click.option('--search', help='Search by project name or description', default=None, type=str)
@click.option('--org', help='filter by Organization name', default=None, type=str)
@click.option('--user', help='filter by User email', default=None, type=str)
@click.option('--status', help='filter by Project status', default=None, type=str)
@login_required
def projects(ctx: click.Context, search: str|None, org: str|None, user: str|None, status: str|None):
    click.echo('Fetching DPM projects...')
    with DpmContextedClient(ctx) as client:
        filters = {
            'search': search,
            'org': org,
            'user': user,
            'status': status
        }
        projects = client.get_projects(**{
            k: v for k, v in filters.items() if v is not None
        })
        
    if not projects:
        click.echo('No projects found!')
    else:
        projects_table = []
        for project in projects:
            projects_table.append([
                project.name, 
                project.org, 
                project.last_revision.spec.member_ref, 
                project.status
            ])
        click.echo(tabulate(projects_table,
            headers=['Name', 'Org', 'User', 'Status']
        ))

@describe.command()
@click.argument('project_name', type=str)
@click.pass_context
@login_required
def project(ctx: click.Context, project_name: str):
    click.echo()
    click.echo(f"Describing project '{project_name}'...")
    click.echo()
    with DpmContextedClient(ctx) as client:
        project = client.get_project(project_name)
    output = [
        ['Name', project.name],
        ['Org', project.org],
        ['User', project.last_revision.spec.member_ref],
        ['Status', project.status],
        ['Created', project.created_at],
    ]
    if project.last_revision is not None:
        output.append(
            ['Last Revision', [
                ['Number', project.last_revision.number],
                ['Created', project.last_revision.created_at],
                ['User', project.last_revision.spec.member_ref],
                ['Status', project.last_revision.status],
            ]]
        )
    if project.stages:
        stages = []
        for stage in project.stages:
            stages.extend([
                ['Name', stage.name],
                ['Status', stage.status],
                ['Message', stage.error_message],
                ['Updated', stage.updated_at],
                
            ])
        output.append(['Stages', stages])
    if project.builds:
        builds = []
        for build in project.builds:
            builds.extend([
                ['Name', build.name],
                ['Stage', build.stage],
                ['Image Digest', build.image_digest.root[:12] if build.image_digest else ''],
                ['Source Commit', build.commit_sha.root[:8] if build.commit_sha else ''],
                ['Workflow', build.workflow_ref],
                ['Status', build.status],
                ['Updated', build.updated_at],
            ])
        output.append(['Builds', builds])
        
    if project.routes:
        routes = []
        for route in project.routes:
            routes.extend([
                ['Name', route.name],
                ['Status', route.status],
                ['URL', route.url],
                ['Custom Domains', os.linesep.join(route.custom_domains)],
            ])
        output.append(['Routes', routes])

    def print_line(output: list, indent: int = 2):
        for l,line in enumerate(output):
            if isinstance(line[1], list):
                click.echo(f"{' ' * indent}{line[0]}:")
                print_line(line[1], indent=indent + 2)
            else:
                prefix = ((' '*(indent-2))+'- ') if indent > 2 and l==0 else (' '* indent)
                click.echo(f"{prefix}{line[0]}: {line[1]}")
    print_line(output)

@get.command()
@click.pass_context
@login_required
def users(ctx: click.Context):
    with DpmContextedClient(ctx) as client:
        for user in client.get_users():
            click.echo(pprint.pprint(user.model_dump()))

@get.command()
@click.pass_context
@click.option('--search', help='Search by route name, project_name or project description', 
              default=None, type=str)
@click.option('--org', help='Organization name', default=None, type=str)
@click.option('--status', help='Route status', default=None, type=str)
@click.option('--stage', help='Stage name', default=None, type=str)
@click.option('--project', help='Project name', default=None, type=str)
@click.option('--open-access', help='Show only open-access routes', default=None, type=bool, is_flag=True)
@click.option('--apps', help='Show only App routes', default=None, type=bool, is_flag=True)
@click.option('--services', help='Show only Service routes', default=None, type=bool, is_flag=True)
@login_required
def routes(ctx: click.Context,
    search: str|None,
    org: str|None,
    stage: str|None,
    status: str|None,
    open_access: bool|None,
    project: bool|None,
    apps: bool|None,
    services: bool|None,
    ):
    routes_table = []
    filters = {
        'org': org,
        'stage': stage,
        'status': status,
        'open_access': open_access,
        'project': project,
        'publish_app': apps,
        'services': services,
        'search': search
    } 
    with DpmContextedClient(ctx) as client:
        for route in client.get_routes(**{
            k: v for k, v in filters.items() if v is not None
        }):
            custom_domains = [f'https://{d}' for d in route.custom_domains]
            route_urls = [route.url] if route.url is not None else []
            routes_table.append([
                route.name,
                route.project,
                route.stage,
                route.status,
                os.linesep.join(custom_domains or route_urls)
            ])            
    if not routes_table:
        click.echo('No routes found!')
    else:
        click.echo(tabulate(routes_table,
            headers=['Name', 'Project', 'Stage', 'Status', 'URL']
        ))