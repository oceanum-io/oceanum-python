import time

import click
import httpx

from functools import update_wrapper
from dotenv import load_dotenv

from .main import main
from .models import DeviceCodeResponse, TokenResponse, Auth0Config


class Auth0Client:
    def __init__(self, config: Auth0Config):
        self.config = config

    def _request(self, method, endpoint, **kwargs) -> httpx.Response:
        url = f'https://{self.config.domain}/' + endpoint
        kwargs.setdefault('headers', {
            'Content-Type': 'application/x-www-form-urlencoded'
        })
        response = httpx.request(method, url, **kwargs)
        response.raise_for_status()
        return response

    def get_device_code(self) -> DeviceCodeResponse:
        data = {
            'client_id': self.config.client_id,
            'scope': 'offline_access'
        }
        response = self._request('POST', 'oauth/device/code', data=data)
        device_code = DeviceCodeResponse(**response.json())
        return device_code
    
    def get_token(self, device_code: str) -> TokenResponse:
        data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
            'client_id': self.config.client_id,
            'device_code': device_code
        }
        response = self._request('POST', 'oauth/token', data=data)
        return TokenResponse(**response.json())
    
    def refresh_token(self, token: TokenResponse) -> TokenResponse:
        if token.refresh_token is None:
            raise Exception('Token does not have a refresh token!')
        data = {
            'grant_type': 'refresh_token',
            'client_id': self.config.client_id,
            'refresh_token': token.refresh_token
        }
        response = self._request('POST', 'oauth/token', data=data)
        return TokenResponse(**response.json())
    
    def wait_for_confirmation(self, device_code: DeviceCodeResponse) -> TokenResponse:
        t0 = time.time()
        while time.time() - t0 < device_code.expires_in:
            try:
                time.sleep(device_code.interval)
                token = self.get_token(device_code.device_code)
            except httpx.HTTPError as e:
                continue
            else:
                break
        token.save()
        return token


@main.group()
def auth():
    pass

def login_required(func):
    @click.pass_context
    def refresh_wrapper(ctx: click.Context, *args, **kwargs):
        if not ctx.obj.token:
            raise Exception('You need to login first!')
        elif ctx.obj.token.created_at.timestamp() + ctx.obj.token.expires_in < time.time():
            token = Auth0Client(config=ctx.obj.auth0).refresh_token(ctx.obj.token)
            token.save()
        return ctx.invoke(func, *args, **kwargs)
    return update_wrapper(refresh_wrapper, func)

@auth.command()
@click.pass_context
def login(ctx: click.Context):
    click.echo('Logging in...')
    client = Auth0Client(ctx.obj.auth0)
    device_code = client.get_device_code()
    click.echo(f'Please visit {device_code.verification_uri_complete} and confirm the code: {device_code.user_code}')
    client.wait_for_confirmation(device_code)
    click.echo('Logged in successfully!')

@auth.command()
@click.pass_context
@login_required
def logout(ctx: click.Context):
    ctx.obj.token.delete()
    click.echo('Logged out successfully!')