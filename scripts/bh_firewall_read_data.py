"""
read_data.py
------------------
This script reads data from DVRPC's postgres db
and inserts it into a Postgres database.

This script won't work outside of the DVRPC firewall; 
for server outside of firewall, a sql dump file will have to 
be used to populate your db with a copy of what is here.

"""

from pg_data_etl import Database
import os

db = Database.from_config("lts", "localhost")
gis_db = Database.from_config("gis", "gis")


def create_schemas(db):
    db.execute(
        """
    CREATE extension if not exists postgis;
    CREATE extension if not exists pgrouting;
    DROP SCHEMA if exists summaries CASCADE;
    CREATE SCHEMA if not exists summaries;
    DROP SCHEMA if exists sidewalk CASCADE;
    CREATE SCHEMA if not exists sidewalk;
    DROP SCHEMA if exists lts CASCADE;
    CREATE SCHEMA if not exists lts;"""
    )


def import_data(
    sql_query: str,
    full_layer_tablename: str,
):
    """
    imports data by creating copies of all tables.
    necessary to copy because of server location; no fdw.
    """

    print(f"initiating import of {full_layer_tablename}, please wait...")

    os.system(
        f"""ogr2ogr -lco GEOMETRY_NAME=geom -sql "{sql_query}" -explodecollections -f "PostgreSQL" -overwrite PG:"host={db.connection_params['host']} user={db.connection_params['un']} dbname={db.connection_params['db_name']} password={db.connection_params['pw']}" -t_srs "EPSG:26918" -f "PostgreSQL" PG:"host={gis_db.connection_params['host']} port={gis_db.connection_params['port']} dbname={gis_db.connection_params['db_name']} user={gis_db.connection_params['un']} password={gis_db.connection_params['pw']}" -nln {full_layer_tablename}"""
    )


def make_low_stress_lts(lts_level: int = 3):
    """Make a low stress network based on a certain threshold. Returns LTS network with
    all segments below specified lts_level (i.e. if write 'lts_level=3', it will create select LTS 1 and 2 as a new table.)
    """
    print(f"creating low stress bike network for lts_{lts_level}...")
    db.execute(
        f"""
        drop table if exists lts.lts_stress_below_{lts_level};
        create table lts.lts_stress_below_{lts_level} as(
        select * from lts_full where lts_score::int < {lts_level});
        """
    )


if __name__ == "__main__":
    create_schemas(db)
    import_data(
        "select *, gid as dvrpc_id from transportation.lts_network where typeno != '22' and typeno != '82'",
        "lts_full",
    )
    import_data(
        """select * from demographics.deccen_2020_block db
            inner join demographics.census_blocks_2020 cb
            on cb.geoid = db.geocode
            """,
        "censusblock2020_demographics",
    )
    import_data(
        "select * from transportation.pedestriannetwork_lines",
        "ped_network",
    )
    import_data(
        "select * from transportation.pedestriannetwork_gaps",
        "ped_network_gaps",
    )
    import_data(
        "select * from boundaries.municipalboundaries",
        "municipalboundaries",
    )
    import_data(
        "select * from planning.eta_essentialservicespts",
        "essential_services",
    )
    import_data(
        "select * from transportation.circuittrails",
        "circuittrails",
    )
    import_data(
        "select * from transportation.crash_newjersey",
        "crash_newjersey",
    )
    import_data(
        "select * from transportation.crash_nj_pedestrians",
        "nj_ped_crash",
    )
    import_data(
        "select * from transportation.passengerrailstations",
        "passengerrailstations",
    )
    import_data(
        "select * from planning.dvrpc_landuse_2015",
        "landuse_2015",
    )
    import_data(
        "select * from economy.nets_2015",
        "nets_2015",
    )
    make_low_stress_lts(4)
    make_low_stress_lts(3)
    make_low_stress_lts(2)
