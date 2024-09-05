from os import linesep
from pathlib import Path
from functools import partial

import yaml
import requests
import click

from ..renderer import Renderer, RenderField
from ..auth import login_required
from ..dpm.client import DeployManagerClient
from .dpm import list_group, describe_group, delete, dpm_group, update_group
from . import models

@update_group.group(name='project', help='Update DPM Project resources')
def update_project_group():
    pass


project_name_option = click.option('-p', '--project', help='Set the project name', required=True, type=str)
project_org_option = click.option('-o', '--org', help='Set the project organization', required=False, type=str)
project_member_option = click.option('-m', '--member', help='Set the project owner email', required=False, type=str)



@list_group.command(name='projects', help='List DPM Projects')
@click.pass_context
@click.option('--search', help='Search by project name or description', default=None, type=str)
@click.option('--org', help='filter by Organization name', default=None, type=str)
@click.option('--user', help='filter by User email', default=None, type=str)
@click.option('--status', help='filter by Project status', default=None, type=str)
@login_required
def list_projects(ctx: click.Context, search: str|None, org: str|None, user: str|None, status: str|None):
    click.echo('Fetching DPM projects...')
    client = DeployManagerClient(ctx)
    filters = {
        'search': search,
        'org': org,
        'user': user,
        'status': status
    }
    projects = client.list_projects(**{
        k: v for k, v in filters.items() if v is not None
    })

    def _status_color(status: str) -> str:
        if status == 'ready':
            return click.style(status.upper(), fg='green')
        elif status == 'degraded':
            return click.style(status.upper(), fg='yellow')
        elif status == 'updating':
            return click.style(status.upper(), fg='cyan')
        elif status == 'error':
            return click.style(status.upper(), fg='red')
        else:
            return click.style(status.upper(), fg='white')
        
    def _color_stage_status(stage: dict) -> str:
        if stage['status'] == 'healthy':
            return click.style(stage['name'], fg='green')
        elif stage['status'] == 'degraded':
            return click.style(stage['name'], fg='yellow')
        elif stage['status'] == 'error':
            return click.style(stage['name'], fg='red')
        elif stage['status'] == 'updating':
            return click.style(stage['name'], fg='cyan')
        else:
            return stage['name']

    fields = [
        RenderField(label='Name', path='$.name'),
        RenderField(label='Org.', path='$.org'),
        RenderField(label='Rev.', path='$.last_revision.number'),
        RenderField(label='Status', path='$.status', mod=_status_color),
        RenderField(label='Stages', path='$.stages.*', mod=_color_stage_status),
    ]
        
    if not projects:
        click.echo('No projects found!')
    else:
        click.echo(Renderer(data=projects, fields=fields).render(output_format='table'))

@dpm_group.command(name='validate', help='Validate DPM Project Specfile')
@click.argument('specfile', type=click.Path(exists=True))
@click.pass_context
@login_required
def validate_project(ctx: click.Context, specfile: click.Path):
    click.echo('Validating DPM Project Spec...')
    client = DeployManagerClient(ctx)
    try:
        client.validate(Path(str(specfile)))
    except Exception as e:
        click.echo(f'ERROR: {e}')
        
    click.echo('OK! Project spec is valid!')

@dpm_group.command(name='deploy', help='Deploy a DPM Project Specfile')
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

    def _merge_secrets(project_spec: models.ProjectSpec, parsed_secrets: list[dict]) -> models.ProjectSpec:
        for secret in parsed_secrets:
            if project_spec.resources is not None:
                if secret['name'] not in [s.name for s in project_spec.resources.secrets]:
                    raise Exception(f"Secret '{secret['name']}' not found in project spec!")
                for existing_secret in project_spec.resources.secrets:
                    if existing_secret.name == secret['name']:
                        if isinstance(existing_secret.data, models.SecretData):
                            if existing_secret.data.root is None:
                                existing_secret.data.root = secret['data']
                            else:
                                existing_secret.data.root.update(secret['data'])
                        else:
                            existing_secret.data.update(secret['data'])
        return project_spec

    client = DeployManagerClient(ctx)
     
    project_spec = client.load_spec(Path(str(specfile)))
    if name:
        project_spec.name = name
    if org is not None:
        project_spec.user_ref = models.UserRef(org)
    if member:
        project_spec.member_ref = member

    if secrets:
        click.echo('Parsing and merging secrets...')
        parsed_secrets = _parse_secrets(secrets)
        project_spec = _merge_secrets(project_spec, parsed_secrets)

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
            click.echo('Waiting for project to be deployed...')
            client.wait_project_deployment(project.name)


@delete.command(name='project')
@click.argument('project_name', type=str)
@click.pass_context
@login_required
def delete_project(ctx: click.Context, project_name: str):
    client = DeployManagerClient(ctx)
    try:
        project = client.get_project(project_name)
    except requests.exceptions.HTTPError as e:
        click.echo(f"Project '{project_name}' not found!")
        return
    else:
        click.confirm(
            f"Deleting project:{linesep}"\
            f"{linesep}"\
            f"Project Name: {project_name}{linesep}"\
            f"Org: {project.org}{linesep}"\
            f"Owner: {project.owner}{linesep}"\
            f"{linesep}"\
            "This will attempt to remove all deployed resources for this project! Are you sure?",
            abort=True)
        client.delete_project(project_name)
        click.echo(f'Project {project_name} deleted! Deployed resources will be removed shortly...')
        


@describe_group.command(name='project', help='Describe a DPM Project')
@click.option('--show-spec', help='Show project spec', default=False, type=bool, is_flag=True)
@click.option('--only-spec', help='Show only project spec', default=False, type=bool, is_flag=True)
@click.argument('project_name', type=str)
@click.pass_context
@login_required
def describe_project(ctx: click.Context, project_name: str, show_spec: bool=False, only_spec: bool=False):
    client = DeployManagerClient(ctx)
    project = client.get_project(project_name)
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
                            'Custom Domains', linesep.join(route.custom_domains)
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


@update_project_group.command(name='description', help='Update project description')
@click.argument('project_name', type=str)
@click.argument('description', type=str)
@click.pass_context
def update_description(ctx: click.Context, project_name: str, description: str):
    client = DeployManagerClient(ctx)
    project = client.get_project(project_name)
    if project is not None:
        project.description = description
        op = models.JSONPatchOpSchema(
            op=models.Op('replace'),
            path='/description',
            value=description
        )
        client.patch_project(project.name, [op])
        click.echo(f"Project '{project_name}' description updated!")
    else:
        click.echo(f"Project '{project_name}' not found!")


@update_project_group.command(name='active', help='Update project status')
@click.argument('project_name', type=str)
@click.argument('active', type=bool)
@click.pass_context
@login_required
def update_active(ctx: click.Context, project_name: str, active: bool):
    client = DeployManagerClient(ctx)
    project = client.get_project(project_name)
    if project is not None:
        op = models.JSONPatchOpSchema(
            op=models.Op('replace'),
            path='/active',
            value=active
        )
        client.patch_project(project.name, [op])
        if active:
            click.echo(f"Project '{project_name}' activated!")
            click.echo(f"Deployed resources will be available shortly!")
            client.wait_project_deployment(project_name)
        else:
            click.echo(f"Project '{project_name}' deactivated, deployed resources will be removed shortly!")
    else:
        click.echo(f"Project '{project_name}' not found!")