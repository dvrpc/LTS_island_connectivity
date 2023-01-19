"""
create_islands.py
------------------
This script reads the LTS network from postgres,
identifies and numbers low stress islands, and
writes a new table with island numbers to postgres.

"""

import geopandas as gpd
from geoalchemy2 import WKTElement
from numpy import full
from env_vars import ENGINE, gis_db, db
import networkx as nx
import pandas as pd


def read_network():
    # eventually might want typeno to get rid of highways and ramps
    ls_network = db.gdf(
        """
            SELECT 
                objectid,
                fromnodeno,
                tonodeno,
                linklts,
                geometry
            FROM "lts_network"
            WHERE linklts <> 'LTS 3'
            AND linklts <> 'LTS 4' ;
            """,
        geom_col="geometry",
    )

    return ls_network


# format link subset so networkx can read it
def identify_islands(ls_network):

    h = ["id", "fn", "tn"]
    links = {}
    for row in ls_network.iterrows():
        betterrow = (row[1][0], row[1][1], row[1][2])
        l = dict(zip(h, betterrow))
        links[(l["fn"], l["tn"])] = l

    # create graph
    G = nx.MultiDiGraph()
    for k, v in links.items():
        G.add_edge(links[k]["fn"], links[k]["tn"])

    G2 = G.to_undirected()
    islands = list(nx.connected_components(G2))

    # number the islands
    # this takes about 20 minutes to run
    results = []
    for k, v in links.items():
        row = [links[k]["id"], links[k]["fn"], links[k]["tn"]]
        for i in range(len(islands)):
            if links[k]["fn"] in islands[i]:
                row.append(i)
                results.append(row)

    # convert list to dataframe and name columns
    islands_df = pd.DataFrame(results)
    islands_df.columns = ["id", "fn", "tn", "island"]

    islands_df.to_sql("lts_network_islands", con=ENGINE, if_exists="replace")
    print("To database: Complete")

    # create new table with islands and geometry so it can be mapped
    q = """drop table if exists islands;
        create table islands AS(
        select lni.*, ln2.geometry, ln2.geom  
        from lts_network_islands lni 
        inner join lts_network ln2
        on ln2.fromnodeno = lni.fn
        and ln2.tonodeno = lni.tn)
        ;
            """

    # create indecies for faster queries
    index_q = """
        create index if not exists idx_islands_geom
        on islands
        using gist (geometry);

        create index if not exists idx_islands_values
        on islands
        using btree (id, fn, tn, island);
        """

    db.execute(q)
    db.execute(index_q)


def main():
    identify_islands(read_network())


if __name__ == "__main__":
    main()
