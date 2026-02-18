# Create a base class for renderering the output of the command line interface.
# The outputs are rendered in the following formats:
# - Table
# - JSON
# - YAML
# The input format should be a dictionary or pydantic a model, 
# plus an indexing of headers for the table format.

from os import linesep
from typing import Type, Literal, Callable, Any
import json
import pprint

import yaml
import jsonpath
import click
from tabulate import tabulate

from pydantic import BaseModel
from oceanum._base import StrictBaseModel

_sty = click.style

output_format_option = click.option(
    '-o','--output', 
    type=click.Choice(['table', 'json', 'yaml']), 
    default='table',
    help='Output format'
)

class RenderField(StrictBaseModel):
    default: Any|None = None
    label: str = 'Name'
    path: str = '$.name'
    mod: Callable = lambda x: x
    lmod: Callable = lambda x: x
    sep: str = ', '

class Renderer:

    def __init__(self, 
        data:list|dict|Type[BaseModel], 
        fields: list[RenderField],
        indent: int = 0,
        output: Literal['table', 'json', 'yaml'] = 'table',
        ignore_fields: list[str] | None = None
    ) -> None:
        self.raw_data = data
        self.parsed_data = self._init_data(data)
        self.fields = fields
        self.indent = indent
        self.ignore_fields = ignore_fields or []

    def _init_data(self, data: list[dict]|list[Type[BaseModel]]|dict|Type[BaseModel]) -> list[dict]:
        """
        Convert the input data to a list of dictionaries.
        """
        dict_data = []
        if isinstance(data, list):
            for item in data:
                if isinstance(item, BaseModel):
                    dict_data.append(item.model_dump(mode='json'))
                else:
                    dict_data.append(item)
        elif isinstance(data, BaseModel):
            dict_data.append(data.model_dump(mode='json'))
        else:
            dict_data.append(data)
        return dict_data
        
    def render_table(self, tablefmt='simple', **dump_kwargs) -> str:
        table_data = []
        headers = [f.label for f in self.fields]
        indent = self.indent if isinstance(self.indent,int) else 0
        for item in self.parsed_data:
            row = []
            for field in self.fields:
                matches = jsonpath.findall(field.path, item)
                if matches:
                    row.append(field.sep.join(str(field.mod(m)) for m in field.lmod(matches)))
                else:
                    row.append(None)
                    click.echo(f"{_sty('WARNING', fg='yellow')}: Could not find a data field for '{field.label}' at path '{field.path}'")
            table_data.append(row)
        if tablefmt == 'plain':
            table_data = zip(headers, table_data[0])
            return tabulate(table_data, tablefmt=tablefmt, **dump_kwargs)
        else:
            return tabulate(table_data, headers=headers, tablefmt=tablefmt)

    def render_json(self, **dump_kwargs) -> str:
        return json.dumps(self.parsed_data, **dump_kwargs)

    def render_yaml(self, **dump_kwargs) -> str:
        return yaml.dump(self.parsed_data, **dump_kwargs)
    
    def render(self, output_format:str='plain', **dump_kwargs) -> str:
        output = ''
        if output_format == 'table':
            output = self.render_table(**dump_kwargs)
        elif output_format == 'json':
            output = self.render_json(**dump_kwargs)
        elif output_format == 'yaml':
            output = self.render_yaml(**dump_kwargs)
        else:
            raise ValueError(f"Invalid format: {output_format}")
        return linesep.join([' '*self.indent+l for l in output.split(linesep)])