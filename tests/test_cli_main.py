
from click.testing import CliRunner

from oceanum.cli import main, auth, dpm, datamesh
from oceanum.cli.storage import storage


def test_main_help():
    result = CliRunner().invoke(main.main, ['--help'])
    assert result.exit_code == 0