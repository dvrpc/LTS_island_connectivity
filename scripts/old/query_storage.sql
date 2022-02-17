WITH buf AS(
	--10m buffer around segment
	select 
		gid,
		st_buffer(geom, 10) buffer
	from test_seg
	where __gid = 468727
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
GROUP BY island

--alternate end--
--grab details of IPD beneath
SELECT DISTINCT(i.*)
FROM ipd_2018 i
JOIN tblB b
ON st_intersects(b.geom, i.geom)

--grab specific details of IPD beneath
SELECT DISTINCT(i.objectid),
	i.namelsad10,
	i.y_class AS youth,
	i.oa_class AS older,
	i.f_class AS female,
	i.rm_class AS racialmin,
	i.em_class AS ethnicmin,
	i.fb_class AS foreignborn,
	i.lep_class AS lep,
	i.d_class AS disabled,
	i.li_class AS lowincome,
	i.u_tpopest AS totpop,
	i.ipd_class AS ipdclass
FROM ipd_2018 i
JOIN tblB b
ON st_intersects(b.geom, i.geom)