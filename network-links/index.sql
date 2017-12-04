--raw
create index yarra__active__asset_id on yarra.active (asset_id);
create index yarra__inactive__asset_id on yarra.inactive (asset_id);

create index yarra__active__yarram on yarra.active using gist (geom);
create index yarra__inactive__yarram on yarra.inactive using gist (geom);

--buffered
create index yarra__active_buffered__asset_id on yarra.active_buffered (asset_id);
create index yarra__active_buffered__yarra on yarra.active_buffered using gist (geom);

create index yarra__inactive_buffered__asset_id on yarra.inactive_buffered (asset_id);
create index yarra__inactive_buffered__yarram on yarra.inactive_buffered using gist (geom);
