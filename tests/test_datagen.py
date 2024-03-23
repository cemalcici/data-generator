# region Global Packages
import pytest
from click.testing import CliRunner
# endregion

# region Local Packages
from data_generator import datagen
# endregion


@pytest.fixture
def runner():
    return CliRunner()


def test_main_succeeds(runner):
    result = runner.invoke(datagen.main)
    assert result.exit_code == 0
