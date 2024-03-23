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
