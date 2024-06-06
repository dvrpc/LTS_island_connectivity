"""
lts CLI
---------------------
This module lets the LTS tool be run from the command line, for those that might
want to use it over the fastapi/react app. 
-------------------------
"""

import click
import geojson
from . import connections


@click.group()
def main():
    """
    lts island connectivity cli
    """
    pass


@main.command()
@click.option(
    "--network_type", default="lts", help="type of network: lts or sidewalk ONLY"
)
@click.option(
    "--geojson_path",
    required=True,
    help="geojson of feature",
)
@click.option("--username", default="cli_user", help="username for db purposes")
@click.option(
    "--highest_comfort_level",
    default=2,
    help="highest comfort level, best to leave at 2",
)
@click.option("--overwrite", help="whether or not to overwrite")
@click.option(
    "--pg_config_filepath",
    help="filepath for pg_config if other than default",
)
def open_geojson(path: str):
    with open(path) as f:
        gj = geojson.loads(f)
        return gj


def cx(
    network_type,
    geojson_path,
    username,
    highest_comfort_level,
    overwrite,
    pg_config_filepath,
):
    """
    Conflates an input table to a base layer.
    """
    geojson = open_geojson(geojson_path)

    connections(
        network_type,
        geojson,
        username,
        highest_comfort_level,
        overwrite,
        pg_config_filepath,
    )
