from network_routing.gaps.segments.generate_islands import generate_islands
from pg_data_etl import Database

db = Database.from_config("localhost")


generate_islands(db, "sidewalk.ped_network", "objectid", "sidewalk_islands", "sidewalk")
generate_islands(
    db,
    "lts.lts_stress_below_2",
    "dvrpc_id",
    "lts1_islands",
    "lts",
)  # generates islands composed of lts 2, 3, and 4 segments
generate_islands(
    db,
    "lts.lts_stress_below_3",
    "dvrpc_id",
    "lts2_islands",
    "lts",
)  # generates islands composed of lts 3, 4 segments
generate_islands(
    db, "lts.lts_stress_below_4", "dvrpc_id", "lts3_islands", "lts"
)  # generates islands only of lts 4 segments

gapslist = [1, 2, 3]
for value in gapslist:
    stressbelow = value + 1
    db.execute(
        f"""drop table if exists lts{value}gaps CASCADE;
            create table lts.lts{value}gaps as select * from lts.lts_full lf where lf.lts_score::int > {value};
            alter table lts.lts_stress_below_{stressbelow} add column if not exists source integer;
            alter table lts.lts_stress_below_{stressbelow} add column if not exists target integer;
            select pgr_createTopology('lts.lts_stress_below_{stressbelow}', 0.0005, 'geom', 'dvrpc_id');
            create or replace view lts.lts{stressbelow}nodes as
                select id, st_centroid(st_collect(pt)) as geom
                from (
                    (select source as id, st_startpoint(geom) as pt
                    from lts.lts_stress_below_{stressbelow}
                    )
                union
                (select target as id, st_endpoint(geom) as pt
                from lts.lts_stress_below_{stressbelow}
                )
                ) as foo
                group by id;
            --adds info needed for pgrouting
            alter table lts.lts_stress_below_{stressbelow} add column if not exists length_m integer;
            update lts.lts_stress_below_{stressbelow} set length_m = st_length(st_transform(geom,26918));
            alter table lts.lts_stress_below_{stressbelow} add column if not exists traveltime_min double precision;
            update lts.lts_stress_below_{stressbelow} set traveltime_min = length_m  / 16000.0 * 60; -- 16 kms per hr, about 10 mph. low range of beginner cyclist speeds

            """
    )

db.execute(
    """
        alter table sidewalk.ped_network add column if not exists source integer;
        alter table sidewalk.ped_network add column if not exists target integer;
        select pgr_createTopology('sidewalk.ped_network', 0.0005, 'geom', 'objectid');
        create or replace view sidewalk.sidewalknodes as 
            select id, st_centroid(st_collect(pt)) as geom
            from (
                (select source as id, st_startpoint(geom) as pt
                from sidewalk.ped_network
                ) 
            union
            (select target as id, st_endpoint(geom) as pt
            from sidewalk.ped_network
            ) 
            ) as foo
            group by id;
        --adds info needed for pgrouting
        alter table sidewalk.ped_network add column if not exists length_m integer;
        update sidewalk.ped_network set length_m = st_length(st_transform(geom,26918));
        alter table sidewalk.ped_network add column if not exists traveltime_min double precision;
        update sidewalk.ped_network set traveltime_min = length_m  / 4820.0 * 60; -- 4.82 kms per hr, about 3 mph. walking speed.

        """
)
