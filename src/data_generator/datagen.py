# region Global Packages
import typer
# endregion

# region Local Packages
from data_generator.helpers import (
    dataframe_to_log
)
# endregion

app = typer.Typer()

app.command('dftolog', help='DataFrame to Log')(dataframe_to_log)

@app.callback()
def callback():
    pass

if __name__ == '__main__':
    app()
