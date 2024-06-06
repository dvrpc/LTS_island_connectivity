"""
lts CLI
---------------------
This module lets the LTS tool be run from the command line, for those that might
want to use it over the fastapi/react app. 
-------------------------
"""

import click
import geojson
<<<<<<< HEAD
from geojson import Feature
from .connections import StudySegment
=======
from . import connections
>>>>>>> gitfix


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
<<<<<<< HEAD
    help="path to geojson of feature(s), handles feature and feature collection",
=======
    help="geojson of feature",
>>>>>>> gitfix
)
@click.option("--username", default="cli_user", help="username for db purposes")
@click.option(
    "--highest_comfort_level",
    default=2,
    help="highest comfort level, best to leave at 2",
)
<<<<<<< HEAD
@click.option("--overwrite", help="whether or not to overwrite", type=bool)
=======
@click.option("--overwrite", help="whether or not to overwrite")
>>>>>>> gitfix
@click.option(
    "--pg_config_filepath",
    help="filepath for pg_config if other than default",
)
<<<<<<< HEAD
=======
def open_geojson(path: str):
    with open(path) as f:
        gj = geojson.loads(f)
        return gj


>>>>>>> gitfix
def cx(
    network_type,
    geojson_path,
    username,
    highest_comfort_level,
    overwrite,
    pg_config_filepath,
):
    """
<<<<<<< HEAD
    Runs the connections.py file, point to a geojson path on your machine.
    """
    geojson = open_geojson(geojson_path)

    if geojson.features:
        for feature in geojson.features:
            StudySegment(
                network_type,
                feature,
                username,
                highest_comfort_level,
                overwrite,
                pg_config_filepath,
            )
    elif geojson.feature:
        StudySegment(
            network_type,
            geojson.feature,
            username,
            highest_comfort_level,
            overwrite,
            pg_config_filepath,
        )
    else:
        print("not sure how to treat this!")


def open_geojson(path: str):
    with open(path) as f:
        s = f.read()
        gj = geojson.loads(s)
        return gj


if __name__ == "__main__":
    cx()
=======
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
>>>>>>> gitfix
