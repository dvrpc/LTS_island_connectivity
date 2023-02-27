from network_routing.gaps.segments.generate_islands import generate_islands
from pg_data_etl import Database

db = Database.from_config("lts", "localhost")
gis_db = Database.from_config("gis", "gis")


generate_islands(db, "ped_network", "sw")
generate_islands(
    db, "lts_stress_below_2", "lts_1"
)  # generates islands composed of lts 2, 3, and 4 segments
generate_islands(
    db, "lts_stress_below_3", "lts_2"
)  # generates islands composed of lts 3, 4 segments
generate_islands(
    db, "lts_stress_below_4", "lts_3"
)  # generates islands only of lts 4 segments

# table cleanup, gets rid of interim tables
stresslist = [2, 3, 4]
for value in stresslist:
    db.execute(f"drop table if exists lts_stress_below_{value};")
#
gapslist = [1, 2, 3]
for value in gapslist:
    db.execute(
            
        f"""drop table if exists lts{value}gaps CASCADE;
            create table lts{value}gaps as select * from public.lts_full lf where lf.lts_score::int > 2;
            alter table lts{value}gaps add column source integer;
            alter table lts{value}gaps add column target integer;
            select pgr_createTopology('lts{value}gaps', 0.0005, 'geom', 'dvrpc_id');
            create or replace view lts{value}nodes as 
                select id, st_centroid(st_collect(pt)) as geom
                from (
                    (select source as id, st_startpoint(geom) as pt
                    from lts{value}gaps
                    ) 
                union
                (select target as id, st_endpoint(geom) as pt
                from lts{value}gaps
                ) 
                ) as foo
                group by id;
            alter table lts{value}gaps add column length_m integer;
            update lts{value}gaps set length_m = st_length(st_transform(geom,26918));
            alter table lts{value}gaps add column traveltime_min double precision;
            update lts{value}gaps set traveltime_min = length_m  / 16000.0 * 60; -- 16 kms per hr, about 10 mph. low range of beginner cyclist speeds

            """
    )
