--basic buffer query
select asset_id, st_buffer(geom, .00002) as geom from yarra.inactive
where asset_id in (1164303, 1131811, 1584281)
--where asset_id = 256;

--NEIGHBORS OF ONE PIPE
select p1.asset_id, array_agg(p2.asset_id) as neighbors
from yarra.inactive as p1
join yarra.inactive as p2 
    on st_intersects(st_buffer(p1.geom, .00002), st_buffer(p2.geom, .00002))
where p1.asset_id = 1164303
and p1.asset_id <> p2.asset_id 
group by p1.asset_id
;
 asset_id |     array_agg     
----------+-------------------
  1164303 | {1131811,1584281}

--NEIGHBORS OF TWO PIPES
select p1.asset_id, array_agg(p2.asset_id) as neighbors
from yarra.inactive as p1
join yarra.inactive as p2 on st_intersects(st_buffer(p1.geom, .00002), st_buffer(p2.geom, .00002))
where p1.asset_id in (256, 14153)
and p1.asset_id <> p2.asset_id
group by p1.asset_id
limit 20
;
 asset_id |       neighbors       
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

select asset_id, distribution_id, st_buffer(geom, .00002) as geom
into yarra.active_buffered
from yarra.active
;

--inactive
with pipe1 as (
select asset_id, geom
from yarra.inactive_buffered
)
select p1.asset_id, array_agg(p2.asset_id) neighbors
into yarra.inactive_neighbors
from pipe1 as p1
join yarra.inactive_buffered as p2 on st_intersects(p1.geom, p2.geom)
where p1.asset_id <> p2.asset_id
group by p1.asset_id
;

--active
with pipe1 as (
select asset_id, geom
from yarra.active_buffered
)
select p1.asset_id, array_agg(p2.asset_id) neighbors
into yarra.active_neighbors
from pipe1 as p1
join yarra.active_buffered as p2 on st_intersects(p1.geom, p2.geom)
where p1.asset_id <> p2.asset_id
group by p1.asset_id
;

--active-inactive
with pipe1 as (
select asset_id, geom
from yarra.active_buffered
)
select p1.asset_id, array_agg(p2.asset_id) neighbors
into yarra.active_inactive_neighbors
from pipe1 as p1
join yarra.inactive_buffered as p2 on st_intersects(p1.geom, p2.geom)
where p1.asset_id <> p2.asset_id
group by p1.asset_id
;

--inactive-active
with pipe1 as (
select asset_id, geom
from yarra.inactive_buffered
)
select p1.asset_id, array_agg(p2.asset_id) neighbors
into yarra.inactive_active_neighbors
from pipe1 as p1
join yarra.active_buffered as p2 on st_intersects(p1.geom, p2.geom)
where p1.asset_id <> p2.asset_id
group by p1.asset_id
;
