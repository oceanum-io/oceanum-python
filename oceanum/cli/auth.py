import time

import click
import httpx

from functools import update_wrapper

from . import settings
from .models import DeviceCodeResponse, TokenResponse
from .base import main

def get_device_code() -> DeviceCodeResponse:
    url = f'https://{settings.AUTH0_DOMAIN}/oauth/device/code'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'client_id': settings.AUTH0_CLIENT_ID,
        'scope': 'openid+offline_access+profile+email',
    }
    response = httpx.post(url, headers=headers, data=data)
    response.raise_for_status()
    device_code = DeviceCodeResponse(**response.json())
    return device_code


def get_token(device_code: str) -> TokenResponse:
    url = f'https://{settings.AUTH0_DOMAIN}/oauth/token'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:device_code',
        'client_id': settings.AUTH0_CLIENT_ID,
        'device_code': device_code
    }
    res = httpx.post(url, headers=headers, data=data)
    res.raise_for_status()
    return TokenResponse(**res.json())

def refresh_token() -> TokenResponse:
    token = TokenResponse.load()
    if token is None:
        raise Exception('You need to login first!')
    if token.refresh_token is None:
        raise Exception('Token does not have a refresh token!')
    url = f'https://{settings.AUTH0_DOMAIN}/oauth/token'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'grant_type': 'refresh_token',
        'client_id': settings.AUTH0_CLIENT_ID,
        'refresh_token': token.refresh_token
    }
    res = httpx.post(url, headers=headers, data=data)
    res.raise_for_status()
    return TokenResponse(**res.json())

def wait_for_confirmation(device_code: DeviceCodeResponse) -> TokenResponse:
    t0 = time.time()
    while time.time() - t0 < device_code.expires_in:
        try:
            time.sleep(device_code.interval)
            token = get_token(device_code.device_code)
        except httpx.HTTPError as e:
            continue
        else:
            break
    token.save()
    return token


def login_required(func):
    @click.pass_context
    def refresh_wrapper(ctx: click.Context, *args, **kwargs):
        if not ctx.obj.token:
            raise Exception('You need to login first!')
        elif ctx.obj.token.created_at.timestamp() + ctx.obj.token.expires_in < time.time():
            token = refresh_token()
            token.save()
        return ctx.invoke(func, *args, **kwargs)
    return update_wrapper(refresh_wrapper, func)

@main.command()
def login():
    click.echo('Logging in...')
    device_code = get_device_code()
    click.echo(f'Please visit {device_code.verification_uri_complete} and confirm the code: {device_code.user_code}')
    wait_for_confirmation(device_code)
    click.echo('Logged in successfully!')

@main.command()
@login_required
def logout(token: TokenResponse):
    token.delete()
    click.echo('Logged out successfully!')