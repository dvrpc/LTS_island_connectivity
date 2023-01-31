"""
read_data.py
------------------
This script reads data from DVRPC's GIS portal
and inserts it into a Postgres database.

Requires geo-enabled postgres database (CREATE EXTENSION postgis;)
"""

import os
from pg_data_etl import Database

db = Database.from_config("lts", "localhost")
gis_db = Database.from_config("gis", "gis")


def import_data(
    sql_query=str,
    geom_col=str,
    full_layer_tablename=str,
    explode=True,
):
    print(f"initiating import of {full_layer_tablename}, please wait...")
    os.system(
        f"""ogr2ogr -lco GEOMETRY_NAME=geom -sql "{sql_query}" -explodecollections -f "PostgreSQL" -overwrite PG:"host={db.connection_params['host']} user={db.connection_params['un']} dbname={db.connection_params['db_name']} password={db.connection_params['pw']}" -t_srs "EPSG:26918" -f "PostgreSQL" PG:"host={gis_db.connection_params['host']} port={gis_db.connection_params['port']} dbname={gis_db.connection_params['db_name']} user={gis_db.connection_params['un']} password={gis_db.connection_params['pw']}" -nln {full_layer_tablename}"""
    )


def make_low_stress_lts(lts_level: int = 3):
    """Make a low stress network based on a certain threshold. Returns LTS network with
    all segments below specified lts_level (i.e. if write 'lts_level=3', it will create select LTS 1 and 2 as a new table.)"""
    db.execute(
        f"""
        drop table if exists lts_stress_below_{lts_level};
        create table lts_stress_below_{lts_level} as(
        select * from lts_full where lts_score::int < {lts_level})"""
    )


if __name__ == "__main__":
    import_data(
        "select *, gid as dvrpc_id from transportation.lts_network where typeno != '22' and typeno != '82'",
        "shape",
        full_layer_tablename="lts_full",
    )
    import_data(
        "select * from demographics.ipd_2020",
        "shape",
        full_layer_tablename="ipd_2020",
    )
    import_data(
        """select * from demographics.deccen_2020_block db
            inner join demographics.census_blocks_2020 cb
            on cb.geoid = db.geocode""",
        "shape",
        full_layer_tablename="censusblock2020_demographics",
    )
    import_data(
        "select * from transportation.pedestriannetwork_lines",
        "shape",
        full_layer_tablename="ped_network",
    )
    import_data(
        "select * from boundaries.municipalboundaries",
        "shape",
        full_layer_tablename="municipalboundaries",
    )
    import_data(
        "select * from planning.eta_essentialservicespts",
        "shape",
        full_layer_tablename="essential_services",
    )

    make_low_stress_lts(4)
    make_low_stress_lts(3)
    make_low_stress_lts(2)
