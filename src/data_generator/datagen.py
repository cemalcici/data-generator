# region Global Packages
import click
# endregion

# region Local Packages
from . import __version__
from data_generator.helpers import (
    dataframe_to_log
)
# endregion


@click.group()
@click.version_option(version=__version__)
def main():
    pass


main.add_command(dataframe_to_log, 'dataframe_to_log')

if __name__ == '__main__':
    main()
