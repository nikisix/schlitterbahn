Geospatial Component of Yarra Water Challenge
DataSci KC
Mon Dec 4 2017
Nick Tomasino

set foldmethod=indent

--QUICK OVERVIEW
    --Two types of pipes: active (184k) and inactive (12k)
    imgcat ~/Desktop/pipes-by-distribution-zone.png


--LOOK AT A FEW PIPES
    --They're lines, not shapes
    --Must buffer the lines and intersect buffers to estimate linkages
    select asset_id, geom from yarra.inactive
    where asset_id in (1164303, 1131811, 1584281)
    ;
    imgcat ~/Desktop/some-pipes.png


--BUFFER SIZING
    --Had to try several buffer sizes before finding the right one
    --Tradeoff: too small and miss legit connections (false negative), too large and false positive.

    select asset_id, st_buffer(geom, .00001) as geom from yarra.inactive
    where asset_id in (1164303 , 1131811 , 1584281 , 35737 , 52839 , 74869)
    ;
    select asset_id, st_buffer(geom, .00005) as geom from yarra.inactive
    where asset_id in (1164303 , 1131811 , 1584281 , 35737 , 52839 , 74869)
    ;
    select asset_id, st_buffer(geom, .00002) as geom from yarra.inactive
    where asset_id in (1164303 , 1131811 , 1584281 , 35737 , 52839 , 74869)
    ;

    imgcat ~/Desktop/buffer-1v2v5.png
    imgcat ~/Desktop/buffer-2v5.png


--NEIGHBORS OF ONE PIPE
    select p1.asset_id, array_agg(p2.asset_id)
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
    imgcat ~/Desktop/pipe-network-linkage.png
    imgcat ~/Desktop/pipe-network-linkage2.png


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
    imgcat ~/Desktop/two-connections.png


--Remove where filter and run all -> TOO SLOW
    Query was taking a long time to run (~hour), so a quick time estimate was in order:

    --Turn timing on (Okay, sometimes tell me the odds)
    postgres=# \timing
    Timing is on.

    --Time our query
    select p1.asset_id, array_agg(p2.asset_id)
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
    (1 row)

    Time: 1680.662 ms (00:01.681)

    --Tabulate how many neighbor sets to do this for
    select count(*) from yarra.inactive;
     count
    -------
     14585

    select count(*) from yarra.active;
     count
    --------
     184297

    --Extrapolate from the time to calculate one pipes neighbors
    --Note: Could've obtained a more accurate estimate by timing multiple neighbor sets and averaging
    select 1.68*14585/3600 as hours_to_run_inactives, 1.68*184297/3600 as hours_to_run_actives;
     hours_to_run_inactives | hours_to_run_actives
    ------------------------+----------------------
         6.8063333333333333 |  86.0052666666666667

    --90 hours too long. Solution due in 48hrs. Pop-quiz what to do?
    --Idea: buffering on n^2 too long buffer each pipe individually and cache
    select asset_id, distribution_id, st_buffer(geom, .00002) as geom
    into yarra.inactive_buffered
    from yarra.inactive
    ;
    --Bonus points: create a geo-index on this cache:
    create index yarra__inactive_buffered__yarram on yarra.inactive_buffered using gist (geom);


--Retry query using buffered-indexed tables:
    select p1.asset_id, array_agg(p2.asset_id)
    from yarra.inactive_buffered as p1
    join yarra.inactive_buffered as p2
        on st_intersects(p1.geom, p2.geom)
    where p1.asset_id = 1164303
    and p1.asset_id <> p2.asset_id
    group by p1.asset_id
    ;
     asset_id |     array_agg
    ----------+-------------------
      1164303 | {1131811,1584281}
    (1 row)

    Time: 2.106 ms
    --Much better


--800 Fold speedup
    select 1680/2.1;
           performance_increase
    ----------------------
     800.0000000000000000


--Brings our overall runtime down from 90 hours to 7 minutes
    select 90*60/800.0;
          runtime
    --------------------
     6.7500000000000000


--OUTPUT
    less ~/data/yarra/neighbors/inactive_neighbors.csv
