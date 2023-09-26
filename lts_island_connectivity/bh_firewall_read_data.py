"""
read_data.py
------------------
This script reads data from DVRPC's postgres db
and inserts it into your local Postgres database.

This script won't work outside of the DVRPC firewall; 
for server outside of firewall, a sql dump file will have to 
be used to populate your db with a copy of what is here.

"""

import os
from settings import (
    DATABASE_URL,
    HOST,
    PORT,
    UN,
    PW,
    DB_NAME,
    GIS_HOST,
    GIS_USER,
    GIS_PASSWORD,
    GIS_DB_NAME,
    GIS_PORT,
)
from sqlalchemy import create_engine, text

engine = create_engine(f"{DATABASE_URL}")


def create_schemas(engine):
    connection = engine.connect()
    query = """
    CREATE extension if not exists postgis;
    CREATE extension if not exists pgrouting;
    DROP SCHEMA if exists summaries CASCADE;
    CREATE SCHEMA if not exists summaries;
    DROP SCHEMA if exists sidewalk CASCADE;
    CREATE SCHEMA if not exists sidewalk;
    DROP SCHEMA if exists lts CASCADE;
    CREATE SCHEMA if not exists lts;
    DROP SCHEMA if exists connect_users cascade;
    CREATE SCHEMA if not exists connect_users;"""

    transaction = connection.begin()

    try:
        connection.execute(text(query))
        transaction.commit()
    except:
        transaction.rollback()
        raise
    finally:
        connection.close()


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
        f"""ogr2ogr -lco GEOMETRY_NAME=geom -sql "{sql_query}" -explodecollections -f "PostgreSQL" -overwrite PG:"host={HOST} port={PORT} user={UN} dbname={DB_NAME} password={PW}" -t_srs "EPSG:26918" -f "PostgreSQL" PG:"host={GIS_HOST} port={GIS_PORT} dbname={GIS_DB_NAME} user={GIS_USER} password={GIS_PASSWORD}" -nln {full_layer_tablename}"""
    )


def make_low_stress_lts(lts_level: int = 3):
    """Make a low stress network based on a certain threshold. Returns LTS network with
    all segments below specified lts_level (i.e. if write 'lts_level=3', it will create select LTS 1 and 2 as a new table.)
    """
    connection = engine.connect()
    transaction = connection.begin()
    query = f"""
        drop table if exists lts.lts_stress_below_{lts_level};
        create table lts.lts_stress_below_{lts_level} as(
        select * from lts.lts_full where lts_score::int < {lts_level});
        """

    try:
        print(f"creating low stress bike network for lts_{lts_level}...")
        connection.execute(text(query))
        transaction.commit()
    except:
        transaction.rollback()
        raise
    finally:
        connection.close()


def make_bike_ped_crash_view():
    connection = engine.connect()
    transaction = connection.begin()
    query = """
        drop materialized view if exists bikepedcrashes;
        CREATE MATERIALIZED VIEW bikepedcrashes as 
            (select st_transform(a.geom,26918) as geom, 
            count(*) filter (where isbycyclist = 'Y') as bike, count(*)
            filter (where isbycyclist is null) as ped from crash_newjersey a 
            inner join nj_ped_crash b 
            on a.casenumber = b.casenumber group by a.geom, a.casenumber);

        """

    try:
        connection.execute(text(query))
        transaction.commit()
    except:
        transaction.rollback()
        raise
    finally:
        connection.close()


def setup_user_table():
    connection = engine.connect()
    transaction = connection.begin()
    query = """
        create table if not exists connect_users.users(
                    id SERIAL primary key,
                    username VARCHAR,
                    hashed_password VARCHAR,
                    is_active BOOL
                );    
        """

    try:
        connection.execute(text(query))
        transaction.commit()
    except:
        transaction.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    create_schemas(engine)
    import_data(
        "select *, gid as dvrpc_id from transportation.lts_network",
        "lts.lts_full",
    )
    import_data(
        """select * from demographics.deccen_2020_block db
            inner join demographics.census_blocks_2020 cb
            on cb.geoid20 = db.geocode
            where cb.dvrpc_reg = 'y'
            """,
        "censusblock2020_demographics",
    )
    import_data(
        "select * from transportation.pedestriannetwork_lines where feat_type != 'UNMARKED'",
        "sidewalk.ped_network",
    )
    import_data(
        "select * from transportation.pedestriannetwork_gaps",
        "sidewalk.ped_network_gaps",
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
    make_bike_ped_crash_view()
    make_low_stress_lts(4)
    make_low_stress_lts(3)
    make_low_stress_lts(2)
