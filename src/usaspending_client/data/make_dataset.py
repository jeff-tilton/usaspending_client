# -*- coding: utf-8 -*-

import sys
import logging
from pathlib import Path

import click


from usaspending_client.utils import log_decorator


PROJECT_DIR = Path(__file__).resolve().parents[2]

LOGGER = logging.getLogger(__name__)
LD = log_decorator(LOGGER)
FORMAT = "%(levelname)s - %(asctime)s - %(name)s - %(message)s"
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format=FORMAT)


@LD
def get_data():
    pass


@LD
def clean_data():
    pass




@click.command()
@click.option('--input_filepath', '-ip',  default=None, type=click.Path(exists=True))
@click.option('--output_filepath', '-op', default=None, type=click.Path())
def make_data(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    LOGGER.info('making final data set from raw data')

    get_data()
    clean_data()
