"""Console script for oceanum library."""

import click


from .models import ContextObject, TokenResponse, Auth0Config

@click.group()
@click.option('--domain', help='Domain to use', default='oceanum.tech')
@click.pass_context
def main(ctx: click.Context, domain:str):
    token = TokenResponse.load()
    if domain == 'oceanum.io':
        auth0_config = Auth0Config(
            domain='oceanum.us.auth0.com',
            client_id=''
        )
    else:
        auth0_config = Auth0Config(
            domain='oceanum-test.au.auth0.com',
            client_id='LTUnEbjjS8Pn1ZTj7VnyYkcTXRShizO9'
        )
    ctx.obj = ContextObject(domain=domain, token=token, auth0=auth0_config)
    click.help_option('-h', '--help')
    click.echo(f'Using domain: {domain}')