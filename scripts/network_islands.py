from network_routing.gaps.segments.generate_islands import generate_islands
from pg_data_etl import Database

db = Database.from_config("lts", "localhost")
gis_db = Database.from_config("gis", "gis")


generate_islands(db, "ped_network", "sw")
generate_islands(db, "lts_stress_below_2", "lts_1") # generates islands composed of lts 2, 3, and 4 segments
generate_islands(db, "lts_stress_below_3", "lts_2") # generates islands composed of lts 3, 4 segments
generate_islands(db, "lts_stress_below_4", "lts_3") # generates islands only of lts 4 segments 

# table cleanup, gets rid of interim tables
stresslist = [2, 3, 4]
for value in stresslist:
    db.execute(f"drop table if exists lts_stress_below_{value};")

gapslist = [1, 2, 3]
for value in gapslist:
    db.execute(
        f"create or replace view lts{value}gaps as select * from public.lts_full lf where lf.lts_score::int > {value}"
    )
