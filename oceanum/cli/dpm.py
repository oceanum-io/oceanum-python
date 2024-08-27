
import click
import os
import yaml
import time
from pathlib import Path
import pprint

from tabulate import tabulate


from ..dpm.client import DPMHttpClient
from ..dpm.models import UserRef, ProjectSchema, SecretSpec, SecretData, ProjectSpec

from .main import main
from .auth import login_required
from .models import TokenResponse
from .renderer import Renderer, output_format_option

class DpmContextedClient:
    def __init__(self, ctx: click.Context) -> None:
        self.ctx = ctx

    def __enter__(self):
        if 'oceanum.' in self.ctx.obj.domain:
            service_url = f'https://dpm.{self.ctx.obj.domain}/api/'
        else:
            # Allow for local development
            service_url = f'http://{self.ctx.obj.domain}/'
        self.dpm = DPMHttpClient(
            token=f"Bearer {self.ctx.obj.token.access_token}",
            service=service_url
        )
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def _wait_project_commit(self, project_name: str) -> bool:
        while True:
            project = self.dpm.get_project(project_name)
            if project.last_revision is not None:
                if project.last_revision.status == 'created':
                    time.sleep(self.dpm._lag)
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
    
    def _wait_stages_start_updating(self, project_name: str) -> ProjectSchema:
        counter = 0
        while True:
            project = self.dpm.get_project(project_name)
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
                time.sleep(self.dpm._lag)
                counter += 1
        return project
    
    def _wait_builds_to_finish(self, project_name: str) -> bool:
        messaged = False
        project = self.dpm.get_project(project_name)
        
        if project.last_revision is not None \
        and project.last_revision.spec.resources \
        and project.last_revision.spec.resources.builds:                
            click.echo('Revision expects one or more images to be built, this can take several minutes...')
            time.sleep(10)
            while True:
                updating_builds = []
                ready_builds = []
                errors = []
                project = self.dpm.get_project(project_name)
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
                time.sleep(self.dpm._lag)
                project = self.dpm.get_project(project_name)
            
        return True
    
    def _wait_stages_finish_updating(self, project_name: str) -> ProjectSchema:
        counter = 0
        click.echo('Waiting for all stages to finish updating...')
        while True:
            project = self.dpm.get_project(project_name)
            updating = any([s.status in ['building'] for s in project.stages])
            all_finished = all([s.status in ['ready', 'error'] for s in project.stages])
            if updating:
                time.sleep(self.dpm._lag)
                continue
            elif all_finished:
                click.echo(f"Project '{project.name}' finished being updated!")
                break
            else:
                time.sleep(self.dpm._lag)
                counter += 1
        return project
    
    def wait_project_deployment(self, project_name: str) -> bool:
        committed = self._wait_project_commit(project_name)
        if committed:
            self._wait_stages_start_updating(project_name)
            self._wait_builds_to_finish(project_name)
            self._wait_stages_finish_updating(project_name)
        return True

@main.group(help='DPM projects Management')
def dpm():
    pass

@dpm.group(name='list', help='List DPM resources')
def list_():
    pass

@dpm.group(help='Describe DPM resources')
def describe():
    pass

@dpm.group(help='Delete DPM resources')
def delete():
    pass

@dpm.command(name='validate', help='Validate DPM Project Specfile')
@click.argument('specfile', type=click.Path(exists=True))
@click.pass_context
@login_required
def validate_project(ctx: click.Context, specfile: click.Path):
    click.echo('Validating DPM Project Spec...')
    with DpmContextedClient(ctx) as client:
        try:
            client.dpm.validate(Path(str(specfile)))
        except Exception as e:
            click.echo(f'ERROR: {e}')
        
    click.echo('OK! Project spec is valid!')

@dpm.command(name='deploy', help='Deploy a DPM Project Specfile')
@click.option('--name', help='Set project name', required=False, type=str)
@click.option('--org', help="Set project's organization namespace to be deployed to", required=False, type=str)
@click.option('--member', help="Set project's owner email address", required=False, type=str)
@click.option('--wait', help='Wait for project to be deployed', default=True)
# Add option to allow passing secrets to the specfile, this will be used to replace placeholders
# can be multiple, e.g. --secret secret-1:key1=value1,key2=value2 --secret secret-2:key2=value2
@click.option('-s','--secrets',help='Replace existing secret data values, i.e secret-name:key1=value1,key2=value2', multiple=True)
@click.argument('specfile', type=click.Path(exists=True))
@click.pass_context
@login_required
def deploy_project(
    ctx: click.Context, 
    specfile: click.Path, 
    name: str|None, 
    org: str|None, 
    member: str|None,
    wait: bool,
    secrets: list[str]
):
    def _parse_secrets(secrets: list[str]) -> list[dict]:
        parsed_secrets = []
        for secret in secrets:
            secret_name, secret_data = secret.split(':')
            secret_data = dict([s.split('=') for s in secret_data.split(',')])
            secret_dict = {'name': secret_name, 'data': secret_data}
            parsed_secrets.append(secret_dict)
        return parsed_secrets

    def _merge_secrets(project_spec: ProjectSpec, parsed_secrets: list[dict]) -> ProjectSpec:
        for secret in parsed_secrets:
            if project_spec.resources is not None:
                if secret['name'] not in [s.name for s in project_spec.resources.secrets]:
                    raise Exception(f"Secret '{secret['name']}' not found in project spec!")
                for existing_secret in project_spec.resources.secrets:
                    if existing_secret.name == secret['name']:
                        if isinstance(existing_secret.data, SecretData):
                            if existing_secret.data.root is None:
                                existing_secret.data.root = secret['data']
                            else:
                                existing_secret.data.root.update(secret['data'])
                        else:
                            existing_secret.data.update(secret['data'])
        return project_spec

    with DpmContextedClient(ctx) as client:
        

        
        project_spec = client.dpm.load_spec(Path(str(specfile)))
        if name:
            project_spec.name = name
        if org is not None:
            project_spec.user_ref = UserRef(org)
        if member:
            project_spec.member_ref = member

        if secrets:
            click.echo('Parsing and merging secrets...')
            parsed_secrets = _parse_secrets(secrets)
            project_spec = _merge_secrets(project_spec, parsed_secrets)

        user_org = project_spec.user_ref or ctx.obj.token.active_org
        user_email = project_spec.member_ref or ctx.obj.token.email
        try: project = client.dpm.get_project(project_spec.name)
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
        client.dpm.deploy_project(project_spec)
        project = client.dpm.get_project(project_spec.name)
        if project.last_revision is not None:
            click.echo(f"Revision #{project.last_revision.number} created successfully!")
            if wait:
                click.echo('Waiting for project to be deployed...')
                client.wait_project_deployment(project.name)

@delete.command(name='project')
@click.argument('project_name', type=str)
@click.pass_context
@login_required
def delete_project(ctx: click.Context, project_name: str):
    click.echo(f'Deleting project {project_name}...')
    with DpmContextedClient(ctx) as client:
        client.dpm.delete_project(project_name)
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
        projects = client.dpm.list_projects(**{
            k: v for k, v in filters.items() if v is not None
        })

    fields = {
        'Name': '$.name',
        'Org': '$.org',
        'Last Author': '$.last_revision.spec.member_ref',
        'Status': '$.status',
    }
        
    if not projects:
        click.echo('No projects found!')
    else:
        click.echo(Renderer(data=projects, fields=fields).render(output_format='table'))

@describe.command(name='project', help='Describe a DPM Project')
@click.option('--show-spec', help='Show project spec', default=False, type=bool, is_flag=True)
@click.option('--only-spec', help='Show only project spec', default=False, type=bool, is_flag=True)
@click.argument('project_name', type=str)
@click.pass_context
@login_required
def describe_project(ctx: click.Context, project_name: str, show_spec: bool=False, only_spec: bool=False):
    with DpmContextedClient(ctx) as client:
        project = client.dpm.get_project(project_name)
    if project.last_revision is not None:
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
    else:
        click.echo(f"Project '{project_name}' does not have any revisions!")

@list_.command(name='users', help='List DPM Users')
@click.pass_context
@login_required
def list_users(ctx: click.Context):
    with DpmContextedClient(ctx) as client:
        fields = {
            'Username': '$.username',
            'Email': '$.email',
            'Current Org': '$.current_org.name',
        }
        users = client.dpm.get_users()
        click.echo(Renderer(data=users, fields=fields).render(output_format='table'))
            

@list_.command(name='routes', help='List DPM Routes')
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

    with DpmContextedClient(ctx) as client:
        fields = {
            'Name': '$.name',
            'Project' : '$.project',
            'Stage': '$.stage',
            'Status': '$.status',
            'URL': '$.url',
        }
        routes =  client.dpm.list_routes(**{
            k: v for k, v in filters.items() if v is not None
        })
        if not routes:
            click.echo('No routes found!')
        else:
            click.echo(Renderer(data=routes, fields=fields).render(output_format=output))

@describe.command(name='route', help='Describe a DPM Service or App Route')
@click.pass_context
@click.argument('route_name', type=str)
@login_required
def describe_route(ctx: click.Context, route_name: str):
    with DpmContextedClient(ctx) as client:
        route = client.dpm.get_route(route_name)
    
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