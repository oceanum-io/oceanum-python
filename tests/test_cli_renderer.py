from unittest import TestCase
import json
import yaml

from oceanum.cli.common.renderer import *

class RendererTests(TestCase):
    
    def test_simple_dict_data(self):
        data = {
            'name' : 'SlimShady',
            'status': 'hidden',
            'notshown': 'notshown'
        }
        fields = {
            'Name': '$.name'
        }
        renderer = Renderer(data, output='table')
        table_output =  renderer.render_table(fields=fields)
        assert 'Name' in table_output
        assert 'SlimShady' in table_output
        assert 'notshown' not in table_output
        json_output = renderer.render_json()
        assert json.loads(json_output.strip("'")) == [data]
        yaml_output = renderer.render_yaml()
        assert yaml.safe_load(yaml_output) == [data]


    
