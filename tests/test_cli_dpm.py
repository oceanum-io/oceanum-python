from unittest import TestCase
from pathlib import Path
from unittest.mock import patch, MagicMock

from oceanum.cli.dpm import *
from oceanum.cli.models import ContextObject, TokenResponse, Auth0Config
from click.testing import CliRunner
from click.globals import get_current_context

class TestDPMCommands(TestCase):

    def setUp(self) -> None:
        self.runner = CliRunner()
        self.specfile = Path(__file__).parent/'data/dpm-project.yaml'
        self.project_spec = DPMHttpClient.load_spec(self.specfile)
        return super().setUp()

    
    def test_deploy_help(self):
        result = self.runner.invoke(dpm, ['deploy', '--help'])
        assert result.exit_code == 0
        
    def test_deploy_empty(self):
        result = self.runner.invoke(dpm, ['deploy'])
        assert result.exit_code != 0
        assert 'Missing argument' in result.output

    def test_validate_specfile(self):
        with patch('oceanum.dpm.client.DPMHttpClient.validate') as mock_validate:
            result = self.runner.invoke(main, ['dpm','validate', str(self.specfile)], catch_exceptions=True)
            assert result.exit_code == 0
            mock_validate.assert_called_once_with(self.specfile)

    def test_deploy_specfile_no_args(self):
        project_spec = DPMHttpClient().load_spec(self.specfile)
        with patch('oceanum.dpm.client.DPMHttpClient.get_project') as mock_get:
            with patch('oceanum.dpm.client.DPMHttpClient.deploy_project') as mock_deploy:
                result = self.runner.invoke(
                    main, ['dpm','deploy', str(self.specfile),'--wait=0']
                )
                assert result.exit_code == 0
                assert mock_deploy.call_args[0][0].name == project_spec.name
                
    def test_deploy_specfile_with_secrets(self):
        secret_overlay = 'test-secret:token=123456'
        with patch('oceanum.dpm.client.DPMHttpClient.get_project') as mock_get:
            with patch('oceanum.dpm.client.DPMHttpClient.deploy_project') as mock_deploy:
                result = self.runner.invoke(
                    main, ['dpm','deploy', str(self.specfile),'-s', secret_overlay,'--wait=0']
                )
                assert result.exit_code == 0
                assert mock_deploy.call_args[0][0].resources.secrets[0].data.root['token'] == '123456'

    def test_deploy_with_org_member(self):
        with patch('oceanum.dpm.client.DPMHttpClient.get_project') as mock_get:
            with patch('oceanum.dpm.client.DPMHttpClient.deploy_project') as mock_deploy:
                result = self.runner.invoke(
                    main, ['dpm','deploy', str(self.specfile),'--org','test','--wait=0','--member=test@test.com']
                )
                assert result.exit_code == 0
                assert mock_deploy.call_args[0][0].user_ref.root == 'test'
                assert mock_deploy.call_args[0][0].member_ref == 'test@test.com'


    def test_describe_help(self):
        result = self.runner.invoke(dpm, ['describe', '--help'])
        assert result.exit_code == 0


    def test_describe_route(self):
        with patch('oceanum.dpm.client.DPMHttpClient.get_route') as mock_get:
            result = self.runner.invoke(main, ['dpm','describe','route','test-route'])
            assert result.exit_code == 0
            mock_get.assert_called_once_with('test-route')

    