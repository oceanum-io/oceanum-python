# Create a base class for renderering the output of the command line interface.
# The outputs are rendered in the following formats:
# - Table
# - JSON
# - YAML
# The input format should be a dictionary or pydantic a model, 
# plus an indexing of headers for the table format.

from typing import Type, Literal
import json
import pprint

import yaml
import jsonpath
import click
from tabulate import tabulate

from pydantic import BaseModel

format_option = click.option(
    '-o','--output', 
    type=click.Choice(['table', 'json', 'yaml']), 
    default='table',
    help='Output format'
)

class Renderer:
    default_fields: dict[str, str] = {
        'Name': '$.name',
    }

    def __init__(self, 
        data: list[dict]|list[Type[BaseModel]]|dict|Type[BaseModel], 
        output: Literal['table', 'json', 'yaml'] = 'table',
        fields: dict[str, str] | None = None
    ) -> None:
        self.raw_data = data
        self.parsed_data = self._init_data(data)
        self.fields = fields or self.default_fields

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
        
    def render_table(self, fields:dict[str, str]|None=None) -> str:
        fields = fields or self.fields
        table_data = []
        for item in self.parsed_data:
            row = []
            for header, path in fields.items():
                match = jsonpath.match(path, item)
                if match:
                    row.append(match.obj)
                else:
                    row.append(None)
                    print(f"WARNING: Could not find a data field for '{header}' at path '{path}'")
            table_data.append(row)
        return tabulate(table_data, headers=list((fields.keys())))

    def render_json(self) -> str:
        return json.dumps(self.parsed_data, indent=4)

    def render_yaml(self) -> str:
        return yaml.dump(self.parsed_data, indent=4)
    
    def render(self, output_format:str='table') -> str:
        if output_format == 'table':
            return self.render_table()
        elif output_format == 'json':
            return self.render_json()
        elif output_format == 'yaml':
            return self.render_yaml()
        else:
            raise ValueError(f"Invalid format: {output_format}")