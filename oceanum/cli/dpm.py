
import click
import httpx

from .base import get
from .auth import login_required
from .models import TokenResponse


class DPMHttpClient:
    def __init__(self, token: TokenResponse, domain: str, prefix: str = 'api/'):
        self.token = token
        self.domain = domain
        self.prefix = prefix

    @property
    def base_url(self):
        if 'oceanum.' in self.domain:
            return f'https://dpm.{self.domain}/'
        return f'http://{self.domain}/'
    
    def _request(self, method, endpoint, **kwargs) -> httpx.Response:
        url = self.base_url + self.prefix + endpoint
        headers =  {
            'Authorization': f'Bearer {self.token.access_token}'
        }
        response = httpx.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        return response
 
    def get(self, endpoint, **kwargs) -> httpx.Response:
        return self._request('GET', endpoint, **kwargs)
    
    def post(self, endpoint, **kwargs) -> httpx.Response:
        return self._request('POST', endpoint, **kwargs)
    
    def put(self, endpoint, **kwargs) -> httpx.Response:
        return self._request('PUT', endpoint, **kwargs)


@get.command()
@click.pass_context
@login_required
def projects(ctx: click.Context):
    click.echo('Fetching DPM projects...')
    client = DPMHttpClient(ctx.obj.token, ctx.obj.domain)
    response = client.get('projects')
    for project in response.json():
        click.echo(f'{project["name"]}: {project["status"]}')