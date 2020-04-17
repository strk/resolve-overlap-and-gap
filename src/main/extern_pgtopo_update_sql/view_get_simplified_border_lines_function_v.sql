CREATE OR REPLACE FUNCTION topo_update.get_simplified_border_lines (
_input_table_name varchar, 
_input_table_geo_column_name varchar, 
_bb geometry, 
_topology_snap_tolerance float, 
_table_name_result_prefix varchar -- The topology schema name where we store store result sufaces and lines from the simple feature dataset,
)
  RETURNS TABLE (
    geo geometry(Linestring)
  )
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  -- This is the boundary geom that contains lines pieces that will added after each single cell is done
  try_update_invalid_rows int;
  tmp_geo1 Geometry;
BEGIN

  -- get the all the line parts based the bb_boundary_outer
  command_string := Format('CREATE TEMP table tmp_data_all_lines AS 
 	WITH rings AS (
 	SELECT ST_ExteriorRing((ST_DumpRings(%3$s)).geom) as geom
 	FROM %1$s v
 	where ST_Intersects(v.%3$s,%2$L)
 	),
 	lines as (select distinct geom from rings)
 	select geom
    from lines where  ST_IsEmpty(geom) is false', 
  _input_table_name, _bb, _input_table_geo_column_name, _topology_snap_tolerance, _bb);
  EXECUTE command_string;
  command_string := Format('create index %1$s on tmp_data_all_lines using gist(geom)', 'idx1' || Md5(ST_AsBinary (_bb)));
  EXECUTE command_string;

 

  -- holds the lines inside bb_boundary_inner
  --#############################
  
 
  CREATE temp TABLE tmp_inner_lines_final_result(geo Geometry(LineString));
 
  insert into tmp_inner_lines_final_result(geo)
  SELECT (ST_Dump(ST_Multi(ST_LineMerge(ST_union(ST_Intersection (rings.geom, _bb)))))).geom as geo
  FROM tmp_data_all_lines AS rings ;
  
  
 -- Try to fix invalid lines
  UPDATE tmp_inner_lines_final_result  r 
  SET geo = ST_MakeValid(r.geo)
  WHERE ST_IsValid (r.geo) = FALSE; 
  GET DIAGNOSTICS try_update_invalid_rows = ROW_COUNT;
  IF  try_update_invalid_rows > 0 THEN
    -- log error lines
    EXECUTE Format('INSERT INTO %s (error_info, geo)
    SELECT %L AS error_info, r.geo
    FROM tmp_inner_lines_final_result  r
    WHERE ST_IsValid (r.geo) = FALSE',_table_name_result_prefix||'_no_cut_line_failed','Failed to make valid input border line in tmp_inner_lines_final_result ');
    
    RETURN QUERY SELECT distinct r.geo FROM tmp_inner_lines_final_result r
    WHERE ST_ISvalid (r.geo);
  ELSE
    -- return the result of inner geos to handled imediatly
    RETURN QUERY SELECT distinct r.geo FROM tmp_inner_lines_final_result r;
    
  END IF; 
  

  -- log error lines
  
  
END
$function$;


--drop table test_tmp_simplified_border_lines_1;

--create table test_tmp_simplified_border_lines_1 as 
--    (select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--     FROM topo_update.get_simplified_border_lines('sl_esh.ar50_utvikling_flate','geo','0103000020E9640000010000000500000000000000084507410000004006F45A4100000000084507410000008013F75A4100000000B0A607410000008013F75A4100000000B0A607410000004006F45A4100000000084507410000004006F45A41','1','test_topo_ar50_t11.ar50_utvikling_flate') g);
--      alter table test_tmp_simplified_border_lines_1 add column id serial;

