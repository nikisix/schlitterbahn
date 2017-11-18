create index geo__active__asset_id on geo.active (asset_id);
create index geo__inactive__asset_id on geo.inactive (asset_id);
create index geo__active__geom on geo.active using gist (geom);
create index geo__inactive__geom on geo.inactive using gist (geom);
--buffered
create index geo__inactive_buffered__asset_id on geo.inactive_buffered (asset_id);
create index geo__inactive_buffered__geom on geo.inactive_buffered using gist (geom);
