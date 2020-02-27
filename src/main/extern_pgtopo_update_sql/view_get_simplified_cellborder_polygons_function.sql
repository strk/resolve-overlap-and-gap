CREATE OR REPLACE FUNCTION topo_update.get_simplified_cellborder_polygons (
_input_table_name varchar, 
_input_table_geo_column_name varchar, 
_bb geometry, _snap_tolerance float8, 
_do_chaikins boolean,
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
  boundary_geom geometry;
  bb_boundary_inner geometry;
  bb_boundary_outer geometry;
  -- This is is a box used to make small glue lines. This lines is needed to make that we don't do any snap out side our own cell
  bb_inner_glue_geom geometry;
  boundary_glue_geom geometry;
  -- TODO add as parameter
  --boundary_with real = 1.5;
  --glue_boundary_with real = 0.5;
  --overlap_width_inner real = 1;
  boundary_with real = _snap_tolerance * 1.5;
  glue_boundary_with real = _snap_tolerance * 0.5;
  overlap_width_inner real = _snap_tolerance;
  rows_affected int;
BEGIN
	
	  
  DROP TABLE IF EXISTS tmp_lines_out;
  CREATE temp TABLE tmp_lines_out(geo Geometry, line_type int);
  

  DROP TABLE IF EXISTS tmp_data_all_lines;
  -- get the all the line parts based the bb_boundary_outer
  command_string := Format('CREATE temp table tmp_data_all_lines AS 
 	WITH rings AS (
 	SELECT ST_ExteriorRing((ST_DumpRings((st_dump(%3$s)).geom)).geom) as geom
 	FROM %1$s v
 	where ST_Intersects(v.%3$s,%2$L)
 	),
 	lines as (select distinct (ST_Dump(geom)).geom as geom from rings)
 	select geom from lines 
 	where  ST_IsEmpty(geom) is false', _input_table_name, ST_ExteriorRing(_bb), _input_table_geo_column_name);
  EXECUTE command_string;

  GET DIAGNOSTICS rows_affected = ROW_COUNT;
  IF  rows_affected > 0 THEN

  DROP TABLE IF EXISTS tmp_inner_lines_merged;
  CREATE temp TABLE tmp_inner_lines_merged AS (
    SELECT (ST_Dump (ST_LineMerge (ST_Union (lg.geom ) ) ) ).geom AS geo, 0 AS line_type
    FROM tmp_data_all_lines AS lg
  );
  
  IF (_snap_tolerance > 0 AND _do_chaikins IS TRUE) THEN
    UPDATE
      tmp_inner_lines_merged lg
    SET geo = ST_simplifyPreserveTopology (topo_update.chaikinsAcuteAngle (lg.geo, 120, 240), _snap_tolerance);
    RAISE NOTICE ' do snap_tolerance % and do do_chaikins %', _snap_tolerance, _do_chaikins;
    -- TODO send paratmeter if this org data or not. _do_chaikins
    --		insert into tmp_inner_lines_merged(geo,line_type)
    --		SELECT e1.geom as geo , 2 as line_type from  topo_ar5_forest_sysdata.edge e1
    --		where e1.geom && bb_inner_glue_geom;
  ELSE
    IF (_snap_tolerance > 0) THEN
      UPDATE
        tmp_inner_lines_merged lg
      SET geo = ST_simplifyPreserveTopology (lg.geo, _snap_tolerance);
      RAISE NOTICE ' do snap_tolerance % and not do do_chaikins %', _snap_tolerance, _do_chaikins;
    END IF;
    --	update tmp_inner_lines_merged lg
    --	set geo = ST_Segmentize(geo, 1);
  END IF;

 

  
 -- Try to fix invalid lines
  UPDATE tmp_inner_lines_merged r 
  SET geo = ST_MakeValid(r.geo)
  WHERE ST_IsValid (r.geo) = FALSE; 
  GET DIAGNOSTICS rows_affected = ROW_COUNT;
  IF  rows_affected > 0 THEN
    -- log error lines
    EXECUTE Format('INSERT INTO %s (error_info, geo)
    SELECT %L AS error_info, r.geo
    FROM tmp_inner_lines_merged r
    WHERE ST_IsValid (r.geo) = FALSE',_table_name_result_prefix||'_no_cut_line_failed','Failed to make valid input border line in tmp_inner_lines_merged');
    
    INSERT INTO tmp_lines_out (geo, line_type)
    SELECT r.geo, 1 AS line_type
    FROM tmp_inner_lines_merged r
    WHERE ST_ISvalid (r.geo);

  ELSE
  
    INSERT INTO tmp_lines_out (geo, line_type)
    SELECT r.geo, 1 AS line_type
    FROM tmp_inner_lines_merged r;
    
  END IF; 
  
  END IF;
    

  -- log error lines
  
  -- return the result of inner geos to handled imediatly
  RETURN QUERY
  SELECT *
  FROM (
    SELECT '{"type": "Feature",' || '"geometry":' || ST_AsGeoJSON (lg3.geo, 10, 2)::json || ',' || '"properties":' || Row_to_json((
        SELECT l FROM (
            SELECT NULL AS "oppdateringsdato") AS l)) || '}' AS json, lg3.geo, 1 AS objectid, lg3.line_type
    FROM tmp_lines_out lg3) as r;
END
$function$;

