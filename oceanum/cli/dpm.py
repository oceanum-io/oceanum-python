
import click
import os
import yaml
import time
from pathlib import Path
import pprint

from tabulate import tabulate

from .main import main
from .auth import login_required
from .models import TokenResponse
from ..dpm.client import DPMHttpClient
from ..dpm.models import UserRef, ProjectSchema

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

@dpm.group(name='list')
def list_():
    pass

@dpm.group()
def describe():
    pass

@dpm.group()
def delete():
    pass

@dpm.command(name='validate')
@click.argument('specfile', type=click.Path(exists=True))
@click.pass_context
@login_required
def validate_project(ctx: click.Context, specfile: click.Path):
    click.echo('Validating DPM Project Spec...')
    with DpmContextedClient(ctx) as client:
        client.validate(Path(str(specfile)))
    click.echo('Project spec is valid!')

@dpm.command(name='deploy')
@click.option('--name', help='Overwrite project name from suplied specfile', required=False, type=str)
@click.option('--org', help='Overwrite organization name', required=False, type=str)
@click.option('--user', help='Overwrite user email', required=False, type=str)
@click.option('--wait', help='Wait for project to be deployed', default=True)
@click.argument('specfile', type=click.Path(exists=True))
@click.pass_context
@login_required
def deploy_project(
    ctx: click.Context, 
    specfile: click.Path, 
    name: str|None, 
    org: str|None, 
    user: str|None,
    wait: bool,
):
    start = time.time()
    with DpmContextedClient(ctx) as client:
        def _wait_project_commit(project_name: str) -> bool:
            while True:
                project = client.get_project(project_name)
                if project.last_revision is not None:
                    if project.last_revision.status == 'created':
                        time.sleep(client._lag)
                        click.echo(f'Waiting for Revision #{project.last_revision.number} to be committed...')
                        continue
                    elif project.last_revision.status == 'no-change':
                        click.echo('No changes to commit, exiting...')
                        return False
                    elif project.last_revision.status == 'failed':
                        click.echo(f"Revision #{project.last_revision.number} failed to commit, exiting...")
                        return False
                    elif project.last_revision.status == 'commited':
                        click.echo(f"Revision #{project.last_revision.number} committed successfully")
                        return True
                else:
                    click.echo('No project revision found, exiting...')
                    break
            return True
        
        def _wait_stages_start_updating(project_name: str) -> ProjectSchema:
            counter = 0
            while True:
                project = client.get_project(project_name)
                updating = any([s.status in ['updating'] for s in project.stages])
                ready_stages = all([s.status in ['ready', 'error'] for s in project.stages])
                if updating:
                    break
                elif counter > 5 and ready_stages:
                    #click.echo(f"Project '{project.name}' finished being updated in {time.time()-start:.2f}s")
                    break
                else:
                    click.echo('Waiting for project to start updating...')
                    pass
                    time.sleep(client._lag)
                    counter += 1
            return project
        
        def _wait_builds_to_finish(project_name: str) -> bool:
            messaged = False
            project = client.get_project(project_name)
            
            if project.last_revision is not None \
            and project.last_revision.spec.resources \
            and project.last_revision.spec.resources.builds:                
                click.echo('Revision expects one or more images to be built, this can take several minutes...')
                time.sleep(10)
                while True:
                    updating_builds = []
                    ready_builds = []
                    errors = []
                    project = client.get_project(project_name)
                    for build in project.builds:
                        if build.status in ['updating', 'pending']:
                            updating_builds.append(build)
                        elif build.status in ['error']:
                            click.echo(f"Build '{build.name}' operation failed!")
                            errors.append(build)
                            ready_builds.append(build)
                        elif build.status in ['success']:
                            click.echo(f"Build '{build.name}' operation succeeded!")
                            ready_builds.append(build)

                    if errors:
                        click.echo(f"Project '{project.name}' failed to build images! Exiting...")
                        return False
                    elif updating_builds and not messaged:
                        click.echo('Waiting for image-builds to finish, this can take several minutes...')
                        messaged = True
                        continue
                    if len(ready_builds) == len(project.builds):
                        click.echo('All builds are finished!')
                        break
                    time.sleep(client._lag)
                    project = client.get_project(project_name)
                
            return True
        
        def _wait_stages_finish_updating(project_name: str) -> ProjectSchema:
            counter = 0
            click.echo('Waiting for all stages to finish updating...')
            while True:
                project = client.get_project(project_name)
                updating = any([s.status in ['building'] for s in project.stages])
                all_finished = all([s.status in ['ready', 'error'] for s in project.stages])
                if updating:
                    time.sleep(client._lag)
                    continue
                elif all_finished:
                    click.echo(f"Project '{project.name}' finished being updated in {time.time()-start:.2f}s")
                    break
                else:
                    time.sleep(client._lag)
                    counter += 1
            return project

        
        project_spec = client.load_spec(Path(str(specfile)))
        if name:
            project_spec.name = name
        if org is not None:
            project_spec.user_ref = UserRef(org)
        if user:
            project_spec.member_ref = user

        user_org = project_spec.user_ref or ctx.obj.token.active_org
        user_email = project_spec.member_ref or ctx.obj.token.email
        try: project = client.get_project(project_spec.name)
        except: project = None
        if project is not None:
            click.echo(f"Updating existing DPM Project:")
        else:
            click.echo(f"Deploying new DPM Project:")
        click.echo(f'  Name: {project_spec.name}')
        click.echo(f'  Org.: {user_org}')
        click.echo(f'  User: {user_email}')
        click.echo()
        click.echo('Safe to Ctrl+C at any time...')
        click.echo()
        client.deploy_project(project_spec)
        project = client.get_project(project_spec.name)
        if project.last_revision is not None:
            click.echo(f"Revision #{project.last_revision.number} created successfully!")
            if wait:
                committed = _wait_project_commit(project.name)
                if committed:
                    _wait_stages_start_updating(project.name)
                    _wait_builds_to_finish(project.name)
                    _wait_stages_finish_updating(project.name)        


@delete.command(name='project')
@click.argument('project_name', type=str)
@click.pass_context
@login_required
def delete_project(ctx: click.Context, project_name: str):
    click.echo(f'Deleting project {project_name}...')
    with DpmContextedClient(ctx) as client:
        client.delete_project(project_name)
    click.echo(f'Project {project_name} deleted!')

@list_.command(name='projects')
@click.pass_context
@click.option('--search', help='Search by project name or description', default=None, type=str)
@click.option('--org', help='filter by Organization name', default=None, type=str)
@click.option('--user', help='filter by User email', default=None, type=str)
@click.option('--status', help='filter by Project status', default=None, type=str)
@login_required
def list_projects(ctx: click.Context, search: str|None, org: str|None, user: str|None, status: str|None):
    click.echo('Fetching DPM projects...')
    with DpmContextedClient(ctx) as client:
        filters = {
            'search': search,
            'org': org,
            'user': user,
            'status': status
        }
        projects = client.list_projects(**{
            k: v for k, v in filters.items() if v is not None
        })
        
    if not projects:
        click.echo('No projects found!')
    else:
        projects_table = []
        for project in projects:
            if project.last_revision is not None:
                projects_table.append([
                    project.name, 
                    project.org, 
                    project.last_revision.spec.member_ref, 
                    project.status
                ])
        click.echo(tabulate(projects_table,
            headers=['Name', 'Org', 'User', 'Status']
        ))

@describe.command(name='project')
@click.option('--show-spec', help='Show project spec', default=False, type=bool, is_flag=True)
@click.option('--only-spec', help='Show only project spec', default=False, type=bool, is_flag=True)
@click.argument('project_name', type=str)
@click.pass_context
@login_required
def describe_project(ctx: click.Context, project_name: str, show_spec: bool=False, only_spec: bool=False):
    with DpmContextedClient(ctx) as client:
        project = client.get_project(project_name)
    if not only_spec:
        click.echo()
        click.echo(f"Describing project '{project_name}'...")
        click.echo()
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
                build_output = [
                    ['Name', build.name],
                    ['Status', build.status],
                    ['Stage', build.stage],
                    ['Workflow', build.workflow_ref],
                    ['Updated', build.updated_at],
                ]
                if build.image_digest is not None:
                    image_digest = getattr(build.image_digest, 'root', None)
                    build_output.append(['Image Digest', image_digest])
                if build.commit_sha is not None:
                    commit_sha = getattr(build.commit_sha, 'root', None)
                    build_output.append(['Source Commit', commit_sha])
                builds.extend(build_output)
            output.append(['Builds', builds])
            
        if project.routes:
            routes = []
            for route in project.routes:
                route_output =  [
                    ['Name', route.name],
                    ['Status', route.status],
                    ['URL', route.url],
                ] 
                if route.custom_domains:
                    route_output.append([
                        'Custom Domains', os.linesep.join(route.custom_domains)
                    ])
                routes.extend(route_output)
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

    if show_spec or only_spec:
        if not only_spec:
            click.echo()
            click.echo('Project Spec:')
            click.echo('---')
        # clear stage status, this will be details above
        for stage in project.last_revision.spec.resources.stages:
            stage.status = None
        click.echo(yaml.dump(project.last_revision.spec.model_dump(
            exclude_none=True, exclude_unset=True, by_alias=True, mode='json'
        )))

@list_.command(name='users')
@click.pass_context
@login_required
def list_users(ctx: click.Context):
    with DpmContextedClient(ctx) as client:
        for user in client.get_users():
            click.echo(pprint.pprint(user.model_dump()))

@list_.command(name='routes')
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
def list_routes(ctx: click.Context,
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
        for route in client.list_routes(**{
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