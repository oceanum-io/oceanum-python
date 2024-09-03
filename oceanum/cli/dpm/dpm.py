
from ..main import main

@main.group(name='dpm', help='DPM projects Management')
def dpm_group():
    pass

@dpm_group.group(name='list', help='List DPM resources')
def list_group():
    pass

@dpm_group.group(name='describe',help='Describe DPM resources')
def describe_group():
    pass

@dpm_group.group(name='delete', help='Delete DPM resources')
def delete():
    pass

@dpm_group.group(name='update',help='Update DPM resources')
def update_group():
    pass