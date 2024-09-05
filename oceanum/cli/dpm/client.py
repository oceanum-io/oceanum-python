
import os
import yaml
import time
from enum import Enum
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Optional

import click
import humanize
import requests
from pydantic import SecretStr, RootModel, Field, model_validator, ValidationError

from . import models
from .utils import format_route_status as _frs, wrn

class RevealedSecretStr(RootModel):
    root: Optional[str|SecretStr] = None

    @model_validator(mode='after')
    def validate_revealed_secret_str(self):
        if isinstance(self.root, SecretStr):
            self.root = self.root.get_secret_value()
        return self            
    
class RevealedSecretData(models.SecretData):
    root: Optional[dict[str, RevealedSecretStr]] = None

class RevealedSecretSpec(models.SecretSpec):
    data: Optional[RevealedSecretData] = None

class RevealedSecretsBuildCredentials(models.BuildCredentials):
    password: Optional[RevealedSecretStr] = None

class RevealedSecretsBuildSpec(models.BuildSpec):
    credentials: Optional[RevealedSecretsBuildCredentials] = None

class RevealedSecretsCustomDomainSpec(models.CustomDomainSpec):
    tls_cert: Optional[RevealedSecretStr] = Field(
        default=None, 
        alias='tlsCert'
    )
    tls_key: Optional[RevealedSecretStr] = Field(
        default=None, 
        alias='tlsKey'
    )

class RevealedSecretsRouteSpec(models.ServiceRouteSpec):
    custom_domains: Optional[list[RevealedSecretsCustomDomainSpec]] = Field(
        default=None, 
        alias='customDomains'
    )

class RevealedSecretsServiceSpec(models.ServiceSpec):
    routes: Optional[list[RevealedSecretsRouteSpec]] = None

class RevealedSecretsImageSpec(models.ImageSpec):
    username: Optional[RevealedSecretStr] = None
    password: Optional[RevealedSecretStr] = None

class RevealedSecretsSourceRepositorySpec(models.SourceRepositorySpec):
    token: Optional[RevealedSecretStr] = None

class RevealedSecretProjectResourcesSpec(models.ProjectResourcesSpec):
    secrets: Optional[list[RevealedSecretSpec]] = None
    build: Optional[RevealedSecretsBuildCredentials] = None
    images: Optional[list[RevealedSecretsImageSpec]] = None
    sources: Optional[list[RevealedSecretsSourceRepositorySpec]] = None

class RevealedSecretsProjectSpec(models.ProjectSpec):
    resources: Optional[RevealedSecretProjectResourcesSpec] = None

def dump_with_secrets(spec: models.ProjectSpec) -> dict:
    spec_dict = spec.model_dump(
        exclude_none=True,
        exclude_unset=True,
        by_alias=True,
        mode='python'
    )
    return RevealedSecretsProjectSpec(**spec_dict).model_dump(
        exclude_none=True,
        exclude_unset=True,
        by_alias=True,
        mode='json'
    )


class DeployManagerClient:
    def __init__(self, ctx: click.Context|None = None, token: str|None = None, service: str|None = None) -> None:
        if ctx is not None:
            self.token = f"Bearer {ctx.obj.token.access_token}"
            self.service = f'https://dpm.{ctx.obj.domain}/api'
        else:
            self.token = token or os.getenv('DPM_TOKEN')
            self.service = service or os.getenv('DPM_API_URL')
        self.ctx = ctx
        self._lag = 2 # seconds

    def _request(self, 
            method: Literal['GET', 'POST', 'PUT','DELETE','PATCH'], 
            endpoint, **kwargs) -> requests.Response:
        assert self.service is not None, 'Service URL is required'
        if self.token is not None:
            headers = kwargs.pop('headers', {})|{
                'Authorization': f'{self.token}'
            }
        url = f"{self.service.removesuffix('/')}/{endpoint}"
        response = requests.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        return response
 
    def _get(self, endpoint, **kwargs) -> requests.Response:
        return self._request('GET', endpoint, **kwargs)
    
    def _post(self, endpoint, **kwargs) -> requests.Response:
        return self._request('POST', endpoint, **kwargs)
    
    def _patch(self, endpoint, **kwargs) -> requests.Response:
        return self._request('PATCH', endpoint, **kwargs)
    
    def _put(self, endpoint, **kwargs) -> requests.Response:
        return self._request('PUT', endpoint, **kwargs)
    
    def _delete(self, endpoint, **kwargs) -> requests.Response:
        return self._request('DELETE', endpoint, **kwargs)
    
    def _wait_project_commit(self, project_name: str) -> bool:
        while True:
            project = self.get_project(project_name)
            if project.last_revision is not None:
                if project.last_revision.status == 'created':
                    time.sleep(self._lag)
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
    
    def _wait_stages_start_updating(self, project_name: str) -> models.ProjectSchema:
        counter = 0
        while True:
            project = self.get_project(project_name)
            updating = any([s.status in ['updating','degraded'] for s in project.stages])
            ready_stages = all([s.status in ['ready', 'error'] for s in project.stages])
            if updating:
                break
            elif counter > 5 and ready_stages:
                #click.echo(f"Project '{project.name}' finished being updated in {time.time()-start:.2f}s")
                break
            else:
                click.echo('Waiting for project to start updating...')
                pass
                time.sleep(self._lag)
                counter += 1
        return project
    
    def _wait_builds_to_finish(self, project_name: str) -> bool:
        messaged = False
        project = self.get_project(project_name)
        
        if project.last_revision is not None \
        and project.last_revision.spec.resources \
        and project.last_revision.spec.resources.builds:                
            click.echo('Revision expects one or more images to be built, this can take several minutes...')
            time.sleep(10)
            while True:
                updating_builds = []
                ready_builds = []
                errors = []
                project = self.get_project(project_name)
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
                time.sleep(self._lag)
                project = self.get_project(project_name)
            
        return True
    
    def _wait_stages_finish_updating(self, project_name: str) -> models.ProjectSchema:
        counter = 0
        click.echo('Waiting for all stages to finish updating...')
        while True:
            project = self.get_project(project_name)
            updating = any([s.status in ['building'] for s in project.stages])
            all_finished = all([s.status in ['healthy', 'error'] for s in project.stages])
            if updating:
                time.sleep(self._lag)
                continue
            elif all_finished:
                click.echo(f"Project '{project.name}' finished being updated!")
                break
            else:
                time.sleep(self._lag)
                counter += 1
        return project
    
    def _check_routes(self, project_name: str) -> bool:
        project = self.get_project(project_name)
        if project.routes:
            for route in project.routes:
                if route.status == 'error':
                    click.echo(f"{wrn('WARNING')}: Route '{route.name}' at stage '{route.stage}' failed to start!")
                    click.echo(f"Status is {_frs(route.status)}, inspect deployment with 'oceanum dpm inspect project {project.name}'!")
                else:
                    click.echo(f"Route '{route.name}' is {_frs(route.status)}!")
                    click.echo(f"Available at URLs:")
                    click.echo(f" * {route.url}")
                    for domain in route.custom_domains:
                        click.echo(f" * https://{domain}/")
                
        return True
    
    def wait_project_deployment(self, project_name: str) -> bool:
        start = time.time()
        committed = self._wait_project_commit(project_name)
        if committed:
            self._wait_stages_start_updating(project_name)
            self._wait_builds_to_finish(project_name)
            self._wait_stages_finish_updating(project_name)
            self._check_routes(project_name)
            delta = timedelta(seconds=time.time()-start)
            click.echo(f"Deployment finished in {humanize.naturaldelta(delta)}.")
        return True
    
    @classmethod
    def load_spec(cls, specfile: Path) -> models.ProjectSpec:
        with specfile.open() as f:
            spec_dict = yaml.safe_load(f)
        return models.ProjectSpec(**spec_dict)
    
    def deploy_project(self, spec: models.ProjectSpec) -> models.ProjectSpec:
        payload = dump_with_secrets(spec)
        response = self._post('projects', json=payload)
        project = response.json()
        return models.ProjectSpec(**project)
    
    def patch_project(self, project_name: str, ops: list[models.JSONPatchOpSchema]) -> models.ProjectSchema:
        payload = [op.model_dump(exclude_none=True, mode='json') for op in ops]
        response = self._patch(f'projects/{project_name}', json=payload)
        project = response.json()
        return models.ProjectSpec(**project)
    
    def delete_project(self, project_id: str) -> requests.Response:
        return self._delete(f'projects/{project_id}')
    
    def get_users(self) -> list[models.UserSchema]:
        response = self._get('users')
        users_json = response.json()
        return [models.UserSchema(**user) for user in users_json]

    def list_projects(self, **filters) -> list[models.ProjectSchema]:
        response = self._get('projects', params=filters or None)
        projects_json = response.json()
        return [models.ProjectSchema(**project) for project in projects_json]
    
    def get_project(self, project_name: str) -> models.ProjectSchema:
        response = self._get(f'projects/{project_name}')
        project_json = response.json()
        return models.ProjectSchema(**project_json)
    
    def list_routes(self, **filters) -> list[models.RouteSchema]:
        response = self._get('routes', params=filters or None)
        routes_json = response.json()
        return [models.RouteSchema(**route) for route in routes_json]
    
    def get_route(self, route_name: str) -> models.RouteSchema:
        response = self._get(f'routes/{route_name}')
        route_json = response.json()
        return models.RouteSchema(**route_json)
    
    def update_route_thumbnail(self, route_name: str, thumbnail: click.File) -> models.RouteSchema:
        files = {'thumbnail': thumbnail}
        response = self._post(f'routes/{route_name}/thumbnail', files=files)
        route_json = response.json()
        return models.RouteThumbnailSchema(**route_json)
    
    def validate(self, specfile: Path) -> models.ProjectSpec:
        with specfile.open() as f:
            spec_dict = yaml.safe_load(f)
        response = self._post('validate', json=spec_dict)
        project_spec = response.json()
        return models.ProjectSpec(**project_spec)

    



