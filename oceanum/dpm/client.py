import os
from typing import Literal
from pathlib import Path

import yaml
import requests


from . import models


class DPMHttpClient:
    def __init__(self,
        token: str|None=None, 
        service: str|None=None,
    ):
        self.token = token or os.getenv('DPM_TOKEN')
        self.service = service or os.getenv('DPM_API_URL')

    def _request(self, 
            method: Literal['GET', 'POST', 'PUT','DELETE'], 
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
    
    def _put(self, endpoint, **kwargs) -> requests.Response:
        return self._request('PUT', endpoint, **kwargs)
    
    def _delete(self, endpoint, **kwargs) -> requests.Response:
        return self._request('DELETE', endpoint, **kwargs)
    
    def load_spec(self, specfile: Path) -> models.ProjectSpec:
        with specfile.open() as f:
            spec_dict = yaml.safe_load(f)
        return models.ProjectSpec(**spec_dict)
    
    def deploy_project(self, spec: models.ProjectSpec) -> models.ProjectSpec:
        response = self._post('projects', json=spec.model_dump(
            exclude_none=True,
            exclude_unset=True,
            by_alias=True,
            mode='json'
        ))
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
    
    def validate(self, specfile: Path) -> models.ProjectSpec:
        with specfile.open() as f:
            spec_dict = yaml.safe_load(f)
        response = self._post('validate', json=spec_dict)
        project_spec = response.json()
        return models.ProjectSpec(**project_spec)