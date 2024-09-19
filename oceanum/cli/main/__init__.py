"""Console script for oceanum library."""

import click
import os

from ..common.models import ContextObject, TokenResponse, Auth0Config

@click.group()
@click.pass_context
def main(ctx: click.Context):
    domain = os.getenv('OCEANUM_DOMAIN', 'oceanum.io')
    if domain == 'oceanum.io':
        auth0_config = Auth0Config(
            domain='auth.oceanum.io',
            client_id='pzXujqmFdkAaVrnsHjn1R7N55GzoIDV2'
        )
    else:
        auth0_config = Auth0Config(
            domain='oceanum-test.au.auth0.com',
            client_id='LTUnEbjjS8Pn1ZTj7VnyYkcTXRShizO9'
        )
    ctx.obj = ContextObject(
        domain=domain, 
        token=TokenResponse.load(domain=domain), 
        auth0=auth0_config
    )
    click.help_option('-h', '--help')