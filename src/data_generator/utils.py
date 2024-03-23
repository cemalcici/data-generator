import os
import click

PACKAGE_DIR = os.path.dirname(os.path.abspath(f'{__file__}'))


def str2bool(ctx, param, value):
    if isinstance(value, bool):
        return value
    elif value.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif value.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise click.BadParameter('Boolean value expected.')


def create_output_folder(
    folder_path: str
):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)


def report_remaining(
    row_count: int,
    repeat: int,
    df_size: int,
    batch_iterval: float,
    repeat_counter: int,
    total_counter: int
) -> None:
    total_time = batch_iterval * df_size * repeat
    remaining_per = 100 - (100 * (total_counter / (repeat * df_size)))
    remaining_time_mins = (total_time -(batch_iterval * row_count * repeat_counter)) / 60
    print(
        f"{total_counter}/{df_size * repeat} processed, " +
        f"%{remaining_per:.2f} will be completed in {remaining_time_mins:.2f} mins."
    )
