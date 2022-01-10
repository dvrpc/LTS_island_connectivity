import psycopg2 as psql

from database import connection

#referenced tables

#tables to be created


#queries
Q_SelectSegments = """
    SELECT
        __gid,
        mun_name,
        geom
    FROM test_seg
"""

#sum length of connected islands
Q_ConIslandLength = """
    WITH buf AS(
        --10m buffer around segment
        select 
            gid,
            st_buffer(geom, 10) buffer
        from test_seg
        where __gid = %d
        ),
    -- select links within the buffer	
    tblA AS(
    SELECT 
        l.gid,
        l.island,
        l.geom
    FROM camco_islands_geo l
    INNER JOIN buf b
    ON st_intersects(l.geom, b.buffer)
    ),
    --select entire islands that are connected by segment
    tblB AS(
    SELECT *
    FROM camco_islands_geo
    WHERE island IN (
        SELECT DISTINCT(island)
        FROM tblA)
        )
    --sum total length of each connected island
    SELECT 
        island,
        sum(st_length(geom))
    FROM tblB
    GROUP BY island;
    """

#grab specific IPD details from tracts beneath
Q_ConIslandStats = """
    WITH buf AS(
        --10m buffer around segment
        select 
            gid,
            st_buffer(geom, 10) buffer
        from test_seg
        where __gid = %d
        ),
    -- select links within the buffer	
    tblA AS(
    SELECT 
        l.gid,
        l.island,
        l.geom
    FROM camco_islands_geo l
    INNER JOIN buf b
    ON st_intersects(l.geom, b.buffer)
    ),
    --select entire islands that are connected by segment
    tblB AS(
    SELECT *
    FROM camco_islands_geo
    WHERE island IN (
        SELECT DISTINCT(island)
        FROM tblA)
        )
    SELECT DISTINCT(i.objectid),
        b.island,
        i.u_tpopest AS totpop,
        i.ipd_class AS ipdclass,
        i.namelsad10 AS name,
        i.y_class AS youth,
        i.oa_class AS older,
        i.f_class AS female,
        i.rm_class AS racialmin,
        i.em_class AS ethnicmin,
        i.fb_class AS foreignborn,
        i.lep_class AS lep,
        i.d_class AS disabled,
        i.li_class AS lowincome
    FROM ipd_2018 i
    JOIN tblB b
    ON st_intersects(b.geom, i.geom);
    """

cur = connection.cursor()
cur.execute(Q_SelectSegments)
segments = cur.fetchall()

results = []
for seg in segments:
    gid = int(seg[0])
    cur.execute(Q_ConIslandLength % gid)
    lengths = cur.fetchall()
    for piece in lengths:
        island = piece[0]
        totlength = piece[1]
        row = [gid, island, totlength] 
    cur.execute(Q_ConIslandStats % gid)
    stats = cur.fetchall()
    for stat in stats:
        print(gid, stat)
        break
        #results.append(row)



#print(results)
#NEXT: what to do when a bunch of tracts are returned for multiple islands? 
# THEN:compile for report out