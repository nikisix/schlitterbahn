--basic buffer query
select asset_id, st_buffer(geom, .00002) as geom from yarra.inactive where asset_id = 256;

--one pipe
with pipe1 as (
select asset_id, geom from yarra.inactive where asset_id = 256
)
select p2.asset_id, p2.geom
from pipe1 as p1
join yarra.inactive as p2 
    on st_intersects(st_buffer(p1.geom, .00002), st_buffer(p2.geom, .00002))
where p1.asset_id <> p2.asset_id 
limit 20
;

--two pipes
with pipe1 as (
select asset_id, geom from yarra.inactive where asset_id in (256, 14153)
)
select p1.asset_id, array_agg(p2.asset_id) --, p2.geom
from pipe1 as p1
join yarra.inactive as p2 on st_intersects(st_buffer(p1.geom, .00002), st_buffer(p2.geom, .00002))
where p1.asset_id <> p2.asset_id
group by p1.asset_id
limit 20
;
 asset_id |       array_agg       
----------+-----------------------
      256 | {14153,25143,1703844}
    14153 | {256,1703844,3240584}


--all the inactive pipes
with pipe1 as (
select asset_id, geom
from yarra.inactive
)
select p1.asset_id, array_agg(p2.asset_id) neighbors
into yarra.neighbors
from pipe1 as p1
join yarra.inactive as p2 on st_intersects(st_buffer(p1.geom, .00002), st_buffer(p2.geom, .00002))
where p1.asset_id <> p2.asset_id
group by p1.asset_id
limit 20
;

--all the active pipes
with pipe1 as (
select asset_id, geom
from yarra.active
)
select p1.asset_id, array_agg(p2.asset_id) neighbors
into yarra.active_neighbors
from pipe1 as p1
join yarra.active as p2 on st_intersects(st_buffer(p1.geom, .00002), st_buffer(p2.geom, .00002))
where p1.asset_id <> p2.asset_id
group by p1.asset_id
limit 20
;

--try saving off the buffer
select asset_id, distribution_id, st_buffer(geom, .00002) as geom
into yarra.inactive_buffered
from yarra.inactive
;

--non-vs-buffered
with pipe1 as (
select asset_id, geom
from yarra.inactive_buffered
limit 3
)
select p1.asset_id, array_agg(p2.asset_id) neighbors
--into yarra.active_neighbors
from pipe1 as p1
join yarra.inactive_buffered as p2 on st_intersects(st_buffer(p1.geom, .00002), st_buffer(p2.geom, .00002))
where p1.asset_id <> p2.asset_id
group by p1.asset_id
limit 20
;
