import click

from .models import ContextObject, TokenResponse

@click.group()
@click.option('--domain', help='Domain to use', default='oceanum.tech') 
@click.pass_context
def main(ctx: click.Context, domain):
    token = TokenResponse.load()
    ctx.obj = ContextObject(domain=domain, token=token)
    click.help_option('-h', '--help')
    click.echo(f'Using domain: {domain}')

@main.group()
def get():
    pass