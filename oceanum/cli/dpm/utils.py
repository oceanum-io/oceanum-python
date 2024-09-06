
import click

def wrn(msg: str) -> None:
    return click.style(msg, fg='yellow')

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