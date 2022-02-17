import psycopg2 as psql
import networkx as nx

from database import connection

#table names
#referenced tables
TBL_LINKS = "camco_ltsp3_links_muni"

#tables to be created
TBL_LINKS_SUB = "camco_lts_sub"
TBL_GROUPS = "camco_lowstress_islands"
TBL_ISLANDS_GEO = "camco_islands_geo"

#indices to be created

cur = connection.cursor()

#create new table with subset of links including only LTS 1 and 2 links
Q_LinkSubset = """
    CREATE TABLE "{0}" AS
        SELECT 
            gid,
            fromnodeno,
            tonodeno,
            geom
        FROM "{1}" 
        WHERE linklts <= 0.3 
        AND linklts >= 0 
        AND typeno NOT IN ('11','12','13','21','22','23','81','82','83','85','86','92');
    COMMIT;
""".format(TBL_LINKS_SUB, TBL_LINKS)
cur.execute(Q_LinkSubset)

#select from table
Q_Select = """
    SELECT gid, fromnodeno, tonodeno
    FROM "{0}"
""".format(TBL_LINKS_SUB)
cur.execute(Q_Select)

#format link subset so networkx can read it
h = ['id', 'fn', 'tn']
links = {}
for row in cur.fetchall():
    betterrow = (row[0], int(row[1]), int(row[2]))
    l = dict(zip(h, betterrow))
    links[(l['fn'], l['tn'])] = l

#create graph
G = nx.MultiDiGraph()
for k, v in links.items():
    G.add_edge(links[k]['fn'], links[k]['tn'])

G2 = G.to_undirected()
islands = list(nx.connected_components(G2))

#number the islands
results = []
for k, v in links.items():
    row = [links[k]['id'], links[k]['fn'], links[k]['tn']]
    for i in range(len(islands)):
        if links[k]['fn'] in islands[i]:
            row.append(i)
            results.append(row)

#get groups back into DB with island numbers
Q_CreateLinkGrpTable = """
    CREATE TABLE public."{0}"(
        gid integer, 
        fromnodeno integer, 
        tonodeno integer, 
        island integer
    );""".format(TBL_GROUPS)
cur.execute(Q_CreateLinkGrpTable)

Q_InsertGrps = """
INSERT INTO public.%s (gid, fromnodeno, tonodeno, island)
VALUES(%d, %d, %d, %d);
"""
for row in results:
    cur.execute((Q_InsertGrps) % (TBL_GROUPS, row[0], row[1], row[2], row[3]))
connection.commit()
del results

#join to geometries
Q_JoinGroupGeo = """
    CREATE TABLE public."{2}" AS(
        SELECT * FROM(
            SELECT 
                t1.*,
                t0.island
            FROM "{0}" AS t0
            LEFT JOIN "{1}" AS t1
            ON t0.gid = t1.gid
        ) AS foo
    );""".format(TBL_GROUPS, TBL_LINKS_SUB, TBL_ISLANDS_GEO)
cur.execute(Q_JoinGroupGeo)
connection.commit()