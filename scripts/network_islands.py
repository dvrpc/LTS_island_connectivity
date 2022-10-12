import geopandas as gpd
from env_vars import db, gis_db
from network_routing.gaps.segments.generate_islands import generate_islands

generate_islands(db, "lts_full_clipped_ls", "lts")
generate_islands(db, "ped_network_clipped", "sw")

# next step: generate gaps, perhaps with some kind of difference function? or is there something in network routing
