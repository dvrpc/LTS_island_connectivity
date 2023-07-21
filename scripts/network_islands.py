from network_routing.gaps.segments.generate_islands import generate_islands
from pg_data_etl import Database

db = Database.from_config("lts", "localhost")
gis_db = Database.from_config("gis", "gis")


generate_islands(db, "ped_network", "sw", "sidewalk")
generate_islands(
    db,
    "lts.lts_stress_below_2",
    "lts_1",
    "lts",
)  # generates islands composed of lts 2, 3, and 4 segments
generate_islands(
    db,
    "lts.lts_stress_below_3",
    "lts_2",
    "lts",
)  # generates islands composed of lts 3, 4 segments
generate_islands(
    db, "lts.lts_stress_below_4", "lts_3", "lts"
)  # generates islands only of lts 4 segments

gapslist = [1, 2, 3]
for value in gapslist:
    stressbelow = value + 1
    db.execute(
        f"""drop table if exists lts{value}gaps CASCADE;
            create table lts.lts{value}gaps as select * from lts_full lf where lf.lts_score::int > {value};
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
