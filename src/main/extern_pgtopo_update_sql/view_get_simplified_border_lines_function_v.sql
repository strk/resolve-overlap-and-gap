CREATE OR REPLACE FUNCTION topo_update.get_simplified_border_lines (
_input_table_name varchar, 
_input_table_geo_column_name varchar, 
_bb geometry, 
_topology_snap_tolerance float, 
_table_name_result_prefix varchar -- The topology schema name where we store store result sufaces and lines from the simple feature dataset,
)
  RETURNS TABLE (
    json text,
    geo geometry,
    objectid integer,
    line_type integer)
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  -- This is the boundary geom that contains lines pieces that will added after each single cell is done
  try_update_invalid_rows int;
BEGIN

  DROP TABLE IF EXISTS tmp_data_all_lines;
  -- get the all the line parts based the bb_boundary_outer
  command_string := Format('CREATE temp table tmp_data_all_lines AS 
 	WITH rings AS (
 	SELECT ST_ExteriorRing((ST_DumpRings((st_dump(%3$s)).geom)).geom) as geom
 	FROM %1$s v
 	where ST_Intersects(v.%3$s,%2$L)
 	),
 	lines as (select distinct (ST_Dump(geom)).geom as geom from rings)
 	select 
     geom, 
     ST_NPoints(geom) as npoints,
     ST_Intersects(geom,%5$L) as touch_outside 
    from lines where  ST_IsEmpty(geom) is false', 
 	_input_table_name, _bb, _input_table_geo_column_name, _topology_snap_tolerance, _bb);
  EXECUTE command_string;
  command_string := Format('create index %1$s on tmp_data_all_lines using gist(geom)', 'idx1' || Md5(ST_AsBinary (_bb)));
  EXECUTE command_string;
  
  -- 1 make line parts for inner box
  -- holds the lines inside bb_boundary_inner
  --#############################
  DROP TABLE IF EXISTS tmp_inner_lines_final_result;
  CREATE temp TABLE tmp_inner_lines_final_result AS (
    SELECT (ST_Dump (ST_Intersection (rings.geom, _bb ) ) ).geom AS geo,
    0 AS line_type
    FROM tmp_data_all_lines AS rings
  );
    
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
    
    INSERT INTO tmp_inner_lines_final_result  (geo, line_type)
    SELECT r.geo, 1 AS line_type
    FROM tmp_inner_lines_final_result r
    WHERE ST_ISvalid (r.geo);
  ELSE
    INSERT INTO tmp_inner_lines_final_result  (geo, line_type)
    SELECT r.geo, 1 AS line_type
    FROM tmp_inner_lines_final_result r;
    
  END IF; 
  

  -- log error lines
  
  -- return the result of inner geos to handled imediatly
  RETURN QUERY
  SELECT *
  FROM (
    SELECT '{"type": "Feature",' || '"geometry":' || ST_AsGeoJSON (lg3.geo, 10, 2)::json || ',' || '"properties":' || Row_to_json((
        SELECT l FROM (
            SELECT NULL AS "oppdateringsdato") AS l)) || '}' AS json, lg3.geo, 1 AS objectid, lg3.line_type
    FROM (
      SELECT l1.geo, l1.line_type
      FROM tmp_inner_lines_final_result  l1
      WHERE ST_IsValid (l1.geo)) AS lg3) AS f;
END
$function$;

