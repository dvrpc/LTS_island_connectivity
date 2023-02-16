"""
read_data.py
------------------
This script reads data from DVRPC's GIS portal
and inserts it into a Postgres database.

Requires geo-enabled postgres database (CREATE EXTENSION postgis;)
"""

from pg_data_etl import Database

db = Database.from_config("lts", "localhost")
gis_db = Database.from_config("gis", "gis")
from planbelt.plan_belt.census import census_pull


def import_data():
    """
    creates a foreign data wrapper, allowing the db to query to make queries from the dvrpc postgres db rather than making a mini-etl pipeline to pull copies in.
    """

    db.execute(
        f"""

    DROP SCHEMA fdw_gis CASCADE;

    CREATE SCHEMA if not exists fdw_gis;

    CREATE EXTENSION if not exists postgres_fdw SCHEMA fdw_gis;

    CREATE SERVER if not exists gis_bridge
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '{gis_db.connection_params['host']}', dbname '{gis_db.connection_params['db_name']}', port '{gis_db.connection_params['port']}');
        
    CREATE USER MAPPING if not exists FOR postgres
        SERVER gis_bridge
        OPTIONS (user '{gis_db.connection_params['un']}', password '{gis_db.connection_params['pw']}');

    IMPORT FOREIGN SCHEMA transportation limit to (circuittrails, pedestriannetwork_lines, lts_network, crash_newjersey, crash_nj_pedestrians ) from server gis_bridge into fdw_gis;
    IMPORT FOREIGN SCHEMA boundaries limit to (municipalboundaries) from server gis_bridge into fdw_gis;
    IMPORT FOREIGN SCHEMA planning limit to (eta_essentialservicespts) from server gis_bridge into fdw_gis;
    IMPORT FOREIGN SCHEMA demographics limit to (ipd_2020, deccen_2020_block, census_blocks_2020) from server gis_bridge into fdw_gis;
    CREATE OR REPLACE VIEW fdw_gis.lts_full as (select *, gid as dvrpc_id from fdw_gis.lts_network where typeno != '22' and typeno != '82');
    CREATE OR REPLACE VIEW fdw_gis.censusblock2020_demographics as (select db.*, cb.geoid, cb.shape from fdw_gis.deccen_2020_block db inner join fdw_gis.census_blocks_2020 cb on cb.geoid = db.geocode); 
    CREATE OR REPLACE VIEW fdw_gis.bikepedcrashes as (select st_transform(a.shape,26918) as shape, count(*) filter (where isbycyclist = 'Y') as bike, count(*) filter (where isbycyclist is null) as ped from fdw_gis.crash_newjersey a inner join fdw_gis.crash_nj_pedestrians b on a.casenumber = b.casenumber group by a.shape, a.casenumber);
    """
    )

def import_census()
    
    """Function to grab census data needed for analysis""" 
    
    census_tables = {
        "B01001_001E": "total",
        "B08014_002E": "zero_car",
        "S1810_C02_001E": "disabled",
        "B09001_001E": "youth",
        "S0101_C01_030E": "older_adults",
        "S1701_C01_042E": "low_income",
        "B02001_002E": "racial_minority",
        "B03002_012E": "ethnic_minority",
    }


def make_low_stress_lts(lts_level: int = 3):
    """Make a low stress network based on a certain threshold. Returns LTS network with
    all segments below specified lts_level (i.e. if write 'lts_level=3', it will create select LTS 1 and 2 as a new table.)"""
    db.execute(
        f"""
        drop table if exists lts_stress_below_{lts_level};
        create table lts_stress_below_{lts_level} as(
        select * from lts_full where lts_score::int < {lts_level})
        """
    )


if __name__ == "__main__":

    import_data()
    make_low_stress_lts(4)
    make_low_stress_lts(3)
    make_low_stress_lts(2)
