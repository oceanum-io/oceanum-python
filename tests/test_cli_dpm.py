from unittest import TestCase
from pathlib import Path
import requests
from unittest.mock import patch, MagicMock
from oceanum.cli.dpm import dpm, project, route, user
from oceanum.cli.dpm.dpm import dpm_group as dpm_cmd
from oceanum.cli.dpm.client import DeployManagerClient
from oceanum.cli.main import main
from oceanum.cli.models import ContextObject, TokenResponse, Auth0Config
from click.testing import CliRunner
from click.globals import get_current_context

class TestDPMCommands(TestCase):

    def setUp(self) -> None:
        self.runner = CliRunner()
        self.specfile = Path(__file__).parent/'data/dpm-project.yaml'
        self.project_spec = DeployManagerClient.load_spec(self.specfile)
        return super().setUp()

    
    def test_deploy_help(self):
        result = self.runner.invoke(dpm_cmd, ['deploy', '--help'])
        assert result.exit_code == 0
        
    def test_deploy_empty(self):
        result = self.runner.invoke(dpm_cmd, ['deploy'])
        assert result.exit_code != 0
        assert 'Missing argument' in result.output

    def test_validate_specfile(self):
        with patch('oceanum.cli.dpm.client.DeployManagerClient.validate') as mock_validate:
            result = self.runner.invoke(main, ['dpm','validate', str(self.specfile)], catch_exceptions=True)
            assert result.exit_code == 0
            mock_validate.assert_called_once_with(self.specfile)

    def test_deploy_specfile_no_args(self):
        project_spec = DeployManagerClient().load_spec(self.specfile)
        with patch('oceanum.cli.dpm.client.DeployManagerClient.get_project') as mock_get:
            with patch('oceanum.cli.dpm.client.DeployManagerClient.deploy_project') as mock_deploy:
                result = self.runner.invoke(
                    main, ['dpm','deploy', str(self.specfile),'--wait=0']
                )
                assert result.exit_code == 0
                assert mock_deploy.call_args[0][0].name == project_spec.name
                
    def test_deploy_specfile_with_secrets(self):
        secret_overlay = 'test-secret:token=123456'
        with patch('oceanum.cli.dpm.client.DeployManagerClient.get_project') as mock_get:
            with patch('oceanum.cli.dpm.client.DeployManagerClient.deploy_project') as mock_deploy:
                result = self.runner.invoke(
                    main, ['dpm','deploy', str(self.specfile),'-s', secret_overlay,'--wait=0']
                )
                assert result.exit_code == 0
                assert mock_deploy.call_args[0][0].resources.secrets[0].data.root['token'] == '123456'

    def test_deploy_with_org_member(self):
        with patch('oceanum.cli.dpm.client.DeployManagerClient.get_project') as mock_get:
            with patch('oceanum.cli.dpm.client.DeployManagerClient.deploy_project') as mock_deploy:
                result = self.runner.invoke(
                    main, ['dpm','deploy', str(self.specfile),'--org','test','--wait=0','--user=test@test.com']
                )
                assert result.exit_code == 0
                assert mock_deploy.call_args[0][0].user_ref.root == 'test'
                assert mock_deploy.call_args[0][0].member_ref == 'test@test.com'


    def test_describe_help(self):
        result = self.runner.invoke(dpm_cmd, ['describe', '--help'])
        assert result.exit_code == 0


    def test_describe_route(self):
        with patch('oceanum.cli.dpm.client.DeployManagerClient.get_route') as mock_get:
            result = self.runner.invoke(main, ['dpm','describe','route','test-route'])
            assert result.exit_code == 0
            mock_get.assert_called_once_with('test-route')
    
    def test_describe_route_not_found(self):
            result = self.runner.invoke(main, ['dpm','describe','route','test-route'])
            assert result.exit_code != 0

    def test_list_routes(self):
        with patch('oceanum.cli.dpm.client.DeployManagerClient.list_routes') as mock_list:
            result = self.runner.invoke(main, ['dpm','list','routes'])
            assert result.exit_code == 0
            mock_list.assert_called_once_with()
    
    def test_list_routes_apps(self):
        with patch('oceanum.cli.dpm.client.DeployManagerClient.list_routes') as mock_list:
            result = self.runner.invoke(main, ['dpm','list','routes','--apps'])
            assert result.exit_code == 0
            mock_list.assert_called_once_with(publish_app=True)
    
    def test_list_routes_services(self):
        with patch('oceanum.cli.dpm.client.DeployManagerClient.list_routes') as mock_list:
            result = self.runner.invoke(main, ['dpm','list','routes','--services'])
            assert result.exit_code == 0
            mock_list.assert_called_once_with(publish_app=False)

    def test_list_routes_open(self):
        with patch('oceanum.cli.dpm.client.DeployManagerClient.list_routes') as mock_list:
            result = self.runner.invoke(main, ['dpm','list','routes','--open'])
            assert result.exit_code == 0
            mock_list.assert_called_once_with(open_access=True)

    def test_list_no_routes(self):
        with patch('oceanum.cli.dpm.client.DeployManagerClient.list_routes') as mock_list:
            mock_list.return_value = []
            result = self.runner.invoke(main, ['dpm','list','routes'])
            assert result.exit_code == 0
