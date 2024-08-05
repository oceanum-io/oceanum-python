from unittest.mock import patch

from click.testing import CliRunner

from oceanum.cli import main, auth, dpm, datamesh, storage, models

def test_auth_help():
    result = CliRunner().invoke(auth.auth, ['--help'])
    assert result.exit_code == 0

def test_auth_login_help():
    result = CliRunner().invoke(auth.login, ['--help'])
    assert result.exit_code == 0

def test_auth_logout_help():
    result = CliRunner().invoke(auth.logout, ['--help'])
    assert result.exit_code == 0

def test_auth_login():
    token = models.TokenResponse(
        access_token='123',
        expires_in=3600,
        token_type='Bearer'
    )
    with patch.object(auth.Auth0Client, 'get_token') as mock_save:
        mock_save.return_value = token
        result = CliRunner().invoke(auth.main, ['auth', 'login'])
    assert result.exit_code == 0