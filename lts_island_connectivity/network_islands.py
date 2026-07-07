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
    tbl = f"lts.lts_stress_below_{stressbelow}"
    tolerance = 0.0005

    db.execute(
        f"""drop table if exists lts.lts{value}gaps CASCADE;
            create table lts.lts{value}gaps as select * from lts.lts_full lf where lf.lts_score::int > {value};

            alter table {tbl} add column if not exists source integer;
            alter table {tbl} add column if not exists target integer;

            drop table if exists {tbl}_vertices_pgr;
            select * into {tbl}_vertices_pgr
            from pgr_extractVertices(
                'select dvrpc_id as id, st_snaptogrid(geom, {tolerance}) as geom
                 from {tbl}
                 order by dvrpc_id'
            );

            update {tbl} e
            set source = v.id
            from {tbl}_vertices_pgr v
            where st_snaptogrid(st_startpoint(e.geom), {tolerance}) = v.geom;

            update {tbl} e
            set target = v.id
            from {tbl}_vertices_pgr v
            where st_snaptogrid(st_endpoint(e.geom), {tolerance}) = v.geom;

            create or replace view lts.lts{stressbelow}nodes as
                select id, st_centroid(st_collect(pt)) as geom
                from (
                    (select source as id, st_startpoint(geom) as pt from {tbl})
                    union
                    (select target as id, st_endpoint(geom) as pt from {tbl})
                ) as foo
                group by id;

            alter table {tbl} add column if not exists length_m integer;
            update {tbl} set length_m = st_length(st_transform(geom,26918));
            alter table {tbl} add column if not exists traveltime_min double precision;
            update {tbl} set traveltime_min = length_m / 16000.0 * 60;
            """
    )
tbl = "sidewalk.ped_network"
tolerance = 0.0005

db.execute(
    f"""
        alter table {tbl} add column if not exists source integer;
        alter table {tbl} add column if not exists target integer;

        drop table if exists {tbl.replace('.', '_')}_vertices_pgr;
        select * into sidewalk.ped_network_vertices_pgr
        from pgr_extractVertices(
            'select objectid as id, st_snaptogrid(geom, {tolerance}) as geom
             from {tbl}
             order by objectid'
        );

        update {tbl} e
        set source = v.id
        from sidewalk.ped_network_vertices_pgr v
        where st_snaptogrid(st_startpoint(e.geom), {tolerance}) = v.geom;

        update {tbl} e
        set target = v.id
        from sidewalk.ped_network_vertices_pgr v
        where st_snaptogrid(st_endpoint(e.geom), {tolerance}) = v.geom;

        create or replace view sidewalk.sidewalknodes as 
            select id, st_centroid(st_collect(pt)) as geom
            from (
                (select source as id, st_startpoint(geom) as pt from {tbl})
                union
                (select target as id, st_endpoint(geom) as pt from {tbl})
            ) as foo
            group by id;

        alter table {tbl} add column if not exists length_m integer;
        update {tbl} set length_m = st_length(st_transform(geom,26918));
        alter table {tbl} add column if not exists traveltime_min double precision;
        update {tbl} set traveltime_min = length_m / 4820.0 * 60;
    """
)