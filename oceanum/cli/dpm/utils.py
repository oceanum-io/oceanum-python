
import click

key = click.style(u"\U0001F511", fg='yellow')

watch = click.style("\u23F1", fg='white')

globe = click.style('\U0001F30D', fg='blue')

spin = click.style(u"\u21BB", fg='cyan')

err = click.style('\u2715', fg='red')

chk = click.style('\u2713', fg='green')

wrn = click.style('\u26A0', fg='yellow')

def format_route_status(status: str) -> str:
    if status == 'online':
        return click.style(status.upper(), fg='green')
    elif status == 'offline':
        return click.style(status.upper(), fg='black')
    elif status == 'pending':
        return click.style(status.upper(), fg='yellow')
    elif status == 'starting':
        return click.style(status.upper(), fg='cyan')
    elif status == 'error':
        return click.style(status.upper(), fg='red', bold=True)
    else:
        return status