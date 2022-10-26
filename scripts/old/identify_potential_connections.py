import geopandas as gpd
from geoalchemy2 import WKTElement
from env_vars import ENGINE
import networkx as nx
import pandas as pd

lts3_links = gpd.read_postgis(
    """
        SELECT 
            objectid,
            fromnodeno,
            tonodeno,
            linklts,
            geometry
        FROM "lts_network" 
        WHERE linklts = 'LTS 3';
        """, con = ENGINE, geom_col="geometry"
)

islands = gpd.read_postgis(
    """
    SELECT *
    FROM islands
    """, con = ENGINE, geom_col="geometry"
)

result_column = []
for index, row in lts3_links.iterrows():

    linkno = row['objectid']
    intersecting_islands = fr"""
        SELECT 
            distinct(island)
        FROM islands
        inner join (
            SELECT 
                st_setsrid(st_buffer(geom, 10), 26918) buffer
            from lts_network
            WHERE objectid = {linkno}) foo
        on  ST_intersects(islands.geometry, foo.buffer);
    """

    conn = ENGINE.connect()
    results = conn.execute(intersecting_islands).all()
    result_row = []
    result_row.append(linkno)
    for i in range(len(results)):
        result_row.append(results[i][0])
    
    result_column.append(result_row)

length_list = []
for i in range(len(result_column)):
    length_list.append(len(result_column[i]))

max(length_list)

df = pd.DataFrame(result_column, columns = ['objectid', '0', '1'])

pd.merge(lts3_links,df, on = 'objectid')
