# region Global Packages
import os
import time
import click
from datetime import datetime
# endregion

# region Local Packages
from data_generator.utils import (
    PACKAGE_DIR,
    str2bool,
    create_output_folder,
    report_remaining
)
from .file_reader import read_source_file
# endregion


# region Command Line Function
@click.command()
@click.option(
    '-s', '--sep', 'sep', 
    required=False, type=str, 
    default=',', show_default=True,
    help='Delimiter.'
)
@click.option(
    '-ls', '--log-sep', 'log_sep',
    required=False, type=str, 
    default=',', show_default=True,
    help='In log file how the fields should separated.'
)
@click.option(
    '-i', '--input', 'input', 
    required=False, type=str, 
    default=os.path.join(PACKAGE_DIR, 'assets', 'input', 'iris.csv'), show_default=True,
    help='Input data path.'
)
@click.option(
    '-o', '--output', 'output',
    required=False, type=str, default=os.path.join(PACKAGE_DIR, 'assets', 'output'), show_default=True,
    help='Output folder.'
)
@click.option(
    '-b', '--batch-interval', 'batch_interval', 
    required=False, type=float, 
    default=0.5, show_default=True,
    help='Time to sleep for every row.'
)
@click.option(
    '-z', '--batch-size', 'batch_size', 
    required=False, type=int, 
    default=10, show_default=True,
    help='How many rows should be in a single log file.'
)
@click.option(
    '-e', '--source-file-extension', 'source_file_extension',
    required=False, type=click.Choice(['csv', 'parquet'], case_sensitive=False),
    default='csv', show_default=True,
    help='How many rows should be in a single log file.'
)
@click.option(
    '-x', '--prefix', 'prefix',
    required=False, type=str, 
    default='my_log', show_default=True,
    help='The prefix of log filename.'
)
@click.option(
    '-oh', '--output-header', 'output_header',
    required=False, type=click.UNPROCESSED, callback=str2bool,
    default=False, show_default=True,
    help='Should log files have header?'
)
@click.option(
    '-ofp', '--is-output-format-parquet', 'is_output_format_parquet',
    required=False, type=click.UNPROCESSED, callback=str2bool,
    default=False, show_default=True,
    help='Is output format be parquet? If True will write parquet format.'
)
@click.option(
    '-idx', '--output-index', 'output_index',
    required=False, type=click.UNPROCESSED, callback=str2bool,
    default=False, show_default=True,
    help='Should log file have index field. Default False, no index'
)
@click.option(
    '-r', '--repeat', 'repeat',
    required=False, type=int,
    default=1, show_default=True,
    help='Round number that how many times dataset generated.'
)
@click.option(
    '-shf', '--shuffle', 'shuffle',
    required=False, type=click.UNPROCESSED, callback=str2bool,
    default=False, show_default=True,
    help='Should dataset shuffled before to generate log? Default False, no shuffle'
)
@click.option(
    '-exc', '--excluded-cols', 'excluded_cols',
    required=False, multiple=True, 
    default=['it_is_impossible_column'], show_default=True,
    help="The columns not to write log file?. Ex: -exc Species -exc PetalWidthCm"
)
def dataframe_to_log(
    sep, log_sep, input, output,
    batch_interval, batch_size,
    source_file_extension,
    prefix, output_header,
    is_output_format_parquet,
    output_index, repeat, shuffle,
    excluded_cols
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
    create_output_folder(output)
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
                report_remaining(
                    row_count, repeat, df_size,
                    batch_interval, repeat_counter,
                    total_counter
                )
                # endregion
            total_counter += 1
        repeat_counter += 1
# endregion
