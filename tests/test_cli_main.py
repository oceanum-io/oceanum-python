
from click.testing import CliRunner

from oceanum.cli import main

def test_main_help():
    result = CliRunner().invoke(main.main, ['--help'])
    assert result.exit_code == 0