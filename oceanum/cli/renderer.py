# Create a base class for renderering the output of the command line interface.
# The outputs are rendered in the following formats:
# - Table
# - JSON
# - YAML
# The input format should be a dictionary or pydantic a model, 
# plus an indexing of headers for the table format.

from os import linesep
from typing import Type, Literal, Callable
import json
import pprint

import yaml
import jsonpath
import click
from tabulate import tabulate

from pydantic import BaseModel

output_format_option = click.option(
    '-o','--output', 
    type=click.Choice(['table', 'json', 'yaml']), 
    default='table',
    help='Output format'
)

class RenderField(BaseModel):
    label: str = 'Name'
    path: str = '$.name'
    mod: Callable = lambda x: x
    sep: str = ', '

class Renderer:
    default_fields: dict[str, str] = {
        'Name': '$.name',
    }

    def __init__(self, 
        data:list|dict|Type[BaseModel], 
        output: Literal['table', 'json', 'yaml'] = 'table',
        fields: list[RenderField] | None = None,
        ignore_fields: list[str] | None = None
    ) -> None:
        self.raw_data = data
        self.parsed_data = self._init_data(data)
        self.fields = fields or self.default_fields
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
        
    def render_table(self, tablefmt='simple') -> str:
        table_data = []
        headers = [f.label for f in self.fields]
        for item in self.parsed_data:
            row = []
            for field in self.fields:
                matches = jsonpath.findall(field.path, item)
                if matches:
                    row.append(field.sep.join(str(field.mod(m)) for m in matches))
                else:
                    row.append(None)
                    print(f"WARNING: Could not find a data field for '{field.label}' at path '{field.path}'")
            table_data.append(row)
        if tablefmt == 'plain':
            table_data = zip(headers, table_data[0])
            return tabulate(table_data, tablefmt=tablefmt)
        else:
            return tabulate(table_data, headers=headers, tablefmt=tablefmt)

    def render_json(self) -> str:
        return json.dumps(self.parsed_data, indent=4)

    def render_yaml(self) -> str:
        return yaml.dump(self.parsed_data, indent=4)
    
    def render(self, output_format:str='plain') -> str:
        if output_format == 'plain':
            return self.render_table(tablefmt='plain')
        elif output_format == 'table':
            return self.render_table()
        elif output_format == 'json':
            return self.render_json()
        elif output_format == 'yaml':
            return self.render_yaml()
        else:
            raise ValueError(f"Invalid format: {output_format}")