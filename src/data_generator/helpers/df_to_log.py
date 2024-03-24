# region Global Packages
import os
import time
import typer
from pathlib import Path
from datetime import datetime
from typing import Optional, List
from typing_extensions import Annotated
# endregion

# region Local Packages
import data_generator.utils as dgutil
from data_generator.utils import (
    PACKAGE_DIR,
    SourceType,
    create_output_folder,
    report_remaining
)
from .file_reader import read_source_file
# endregion


# region Command Line Function
def dataframe_to_log(
    sep: Annotated[
        Optional[str],
        typer.Option('--sep', '-s', help='Delimiter.')
    ] = ',',
    log_sep: Annotated[
        Optional[str],
        typer.Option('--log-sep', '-ls', help='In log file how the fields should separated.')
    ] = ',',
    input: Annotated[
        Optional[Path],
        typer.Option('--input', '-i', help='Input data path.')
    ] = os.path.join(dgutil.PACKAGE_DIR, 'assets', 'input', 'iris.csv'),
    output: Annotated[
        Optional[Path],
        typer.Option('--output', '-o', help='Output folder')
    ] = os.path.join(dgutil.PACKAGE_DIR, 'assets', 'output'),
    batch_interval: Annotated[
        Optional[float],
        typer.Option('--batch-interval', '-b', help='Time to sleep for every row.')
    ] = 0.5,
    batch_size: Annotated[
        Optional[int],
        typer.Option('--batch-size', '-z', help='How many rows should be in a single log file.')
    ] = 10,
    source_file_extension: Annotated[
        Optional[dgutil.SourceType],
        typer.Option('--source-file-extension', '-e', help='How many rows should be in a single log file.', case_sensitive=False)
    ] = dgutil.SourceType.csv,
    prefix: Annotated[
        Optional[str],
        typer.Option('--prefix', '-x', help='The prefix of log filename.')
    ] = 'my_log',
    output_header: Annotated[
        bool,
        typer.Option('--output-header/--no-output-header', '-oh/-OH', help='Should log files have header?')
    ] = False,
    is_output_format_parquet: Annotated[
        bool,
        typer.Option('--output-parquet/--no-output-parquet', '-op/-OP', help='Is output format be parquet?')
    ] = False,
    output_index: Annotated[
        bool,
        typer.Option('--output-index/--no-output-index', '-oi/-OI', help='Should log file have index field?')
    ] = False,
    repeat: Annotated[
        Optional[int],
        typer.Option('--repeat', '-r', help='Round number that how many times dataset generated.')
    ] = 1,
    shuffle: Annotated[
        bool,
        typer.Option('--shuffle/--no-shuffle', '-shf/-SHF', help='Should dataset shuffled before to generate log? Default False, no shuffle')
    ] = False,
    excluded_cols: Annotated[
        Optional[List[str]],
        typer.Argument(help='The columns not to write log file?. Ex: Species PetalWidthCm')
    ] = None
):
    print(
        f"dataframe_to_log(\n\tsep='{sep}',\n\tlog_sep='{log_sep}'," +
        f"\n\tinput='{input}',\n\toutput='{output}'," +
        f"\n\tbatch_interval={batch_interval},\n\tbatch_size={batch_size},"+ 
        f"\n\tsource_file_extension='{source_file_extension}'," +
        f"\n\tprefix='{prefix}',\n\toutput_header={output_header}," + 
        f"\n\tis_output_format_parquet={is_output_format_parquet}," +
        f"\n\toutput_index={output_index},\n\trepeat={repeat}," + 
        f"\n\tshuffle={shuffle},\n\texcluded_cols={excluded_cols}\n)"
    )
    print(f"Starting in {batch_interval * batch_size} seconds... ")
    
    # region Create DataFrame
    df = read_source_file(
        input, sep, 
        source_file_extension, 
        shuffle, excluded_cols
    )
    # endregion
    
    # region Create Output Folder    
    dgutil.create_output_folder(output)
    # endregion
    
    # region Prepared Variables for Loop
    df_size = len(df)
    time_list_for_each_batch = []
    repeat_counter = 1
    total_counter = 1
    # endregion
    
    for _ in range(repeat):
        batch_counter = 0
        for row_count in range(1, df_size + 1):
            time.sleep(batch_interval)
            time_list_for_each_batch.append(datetime.now())
            
            if (row_count % batch_size == 0) or (row_count == df_size):
                # region Created Batch DataFrame
                df_batch = df.iloc[batch_counter:row_count, :].copy()
                df_batch['event_time'] = time_list_for_each_batch
                # endregion
                
                # region Truncated Time List
                time_list_for_each_batch = []
                # endregion
                
                # region Wrote Batch DataFrame Log
                timestr = time.strftime("%Y%m%d-%H%M%S")
                if is_output_format_parquet:
                    df_batch.to_parquet(
                        os.path.join(output, f'{prefix}_{timestr}.parquet'),
                        engine='pyarrow', index=output_index
                    )
                else:
                    df_batch.to_csv(
                        os.path.join(output, f'{prefix}_{timestr}.csv'),
                        header=output_header, index=output_index,
                        index_label='ID', encoding='utf-8', sep=log_sep
                    )
                # endregion
                
                # region Offseted Row Count Per Batch Size
                batch_counter = row_count
                # endregion
                
                # region Reported Remaining
                dgutil.report_remaining(
                    row_count, repeat, df_size,
                    batch_interval, repeat_counter,
                    total_counter
                )
                # endregion
            total_counter += 1
        repeat_counter += 1
# endregion
