CREATE OR REPLACE FUNCTION topo_update.get_simplified_border_lines (
_input_table_name varchar, 
_input_table_geo_column_name varchar, 
_bb geometry, 
_topology_snap_tolerance float, 
_table_name_result_prefix varchar -- The topology schema name where we store store result sufaces and lines from the simple feature dataset,
)
  RETURNS TABLE (
    geo geometry,
    outer_border_line boolean
  )
  LANGUAGE 'plpgsql'
  AS $function$
DECLARE
  command_string text;
  
  -- This is used to sure that no lines can snap to each other between two cells
  -- The size wil the this value multiplied by _topology_snap_tolerance;
  -- TODO make this as parameter
  cell_boundary_tolerance_with_multi real = 6;
  
  -- This is the boundary geom that contains lines pieces that will added after each single cell is done
  boundary_geom geometry;
  bb_boundary_inner geometry;
  bb_boundary_outer geometry;
  
  -- This is is a box used to make small glue lines. This lines is needed to make that we don't do any snap out side our own cell
  bb_inner_glue_geom geometry;
  boundary_glue_geom geometry;

  -- The inpux
  boundary_with float = _topology_snap_tolerance*1.1;
  glue_boundary_with float = _topology_snap_tolerance * cell_boundary_tolerance_with_multi;
  overlap_width_inner float = 0;
  try_update_invalid_rows int;
  
  _max_point_in_line int = 10000;
  
  cell_id int;
  
BEGIN
	
	
  -- buffer in to work with geom that lines are only meter from the border
  -- will only work with polygons
  -- make the the polygon that contains lines	that will be added in the post process
  bb_boundary_outer := ST_Expand (_bb, boundary_with);
  bb_boundary_inner := ST_Expand (_bb, (boundary_with * - 1));
  boundary_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (ST_Expand (_bb, boundary_with)) AS outer_ring), ARRAY (
        SELECT ST_ExteriorRing (ST_Expand (_bb, ((boundary_with + overlap_width_inner) * - 1))) AS inner_rings));
        
  -- make the the polygon that contains lines is used a glue lines
  bb_inner_glue_geom := ST_Expand (_bb, ((boundary_with + glue_boundary_with) * - 1));
  boundary_glue_geom := ST_MakePolygon ((
      SELECT ST_ExteriorRing (bb_boundary_inner) AS outer_ring), ARRAY (
        SELECT ST_ExteriorRing (bb_inner_glue_geom) AS inner_rings));
  -- holds the lines inside bb_boundary_inner
  -- get the all the line parts based the bb_boundary_outer
  command_string := Format('CREATE TEMP TABLE tmp_data_all_lines AS WITH 
    rings AS (
 	  SELECT ST_ExteriorRing((ST_DumpRings((st_dump(%3$s)).geom)).geom) as geom
 	  FROM %1$s v
 	  where ST_Intersects(v.%3$s,%2$L)
 	),
 	lines as (
      SELECT( ST_Dump(ST_Multi(ST_LineMerge(ST_union(rings.geom))))).geom 
      from rings
    ),
    tmp_data_this_cell_lines AS (
      SELECT 
      case WHEN ST_IsValid (r.geom) = FALSE THEN ST_MakeValid(r.geom)
      ELSE r.geom
      END as geom,
      min_cell_id
      FROM 
      (
      SELECT min(g.id) as min_cell_id, l.geom FROM
      lines l,
      %5$s g
      where ST_Intersects(l.geom,g.%3$s)
      group by l.geom
      ) AS r 
      WHERE ST_IsEmpty(r.geom) is false      
 	)
    select geom, ST_NPoints(geom) AS npoints, min_cell_id from tmp_data_this_cell_lines
    ', 
 	_input_table_name, 
 	_bb, 
 	_input_table_geo_column_name, 
 	_topology_snap_tolerance,
 	_table_name_result_prefix||'_grid', 
 	ST_ExteriorRing(_bb)
 	);
  EXECUTE command_string;

  
  command_string := Format('select id from %1$s g WHERE g.%2$s = %3$L ',
  _table_name_result_prefix||'_grid', 
  _input_table_geo_column_name, 
  _bb
 	);
  EXECUTE command_string into cell_id;

  command_string := Format('create index on tmp_data_all_lines using gist(geom)', 'idxtmp_data_all_lines_geom' || Md5(ST_AsBinary (_bb)));
  EXECUTE command_string;
  
  command_string := Format('create index on tmp_data_all_lines(min_cell_id)', 'idxtmp_data_all_lines_min_cell_id' || Md5(ST_AsBinary (_bb)));
  EXECUTE command_string;

   
  -- log error lines
  EXECUTE Format('WITH error_lines AS 
    (DELETE FROM tmp_data_all_lines r where r.min_cell_id = %1$s and ST_IsValid(r.geom) = false RETURNING geom) 
    INSERT INTO %2$s (error_info, geo)
    SELECT %3$L AS error_info, r.geom as geo
    FROM error_lines r',
    cell_id,
   _table_name_result_prefix||'_no_cut_line_failed',
   'Failed to make valid input border line for tmp_boundary_lines_merged' );
  
 

  -- insert lines with more than max point
  EXECUTE Format('WITH long_lines AS 
    (DELETE FROM tmp_data_all_lines r where npoints >  %2$s and ST_Intersects(geom,%3$L) and min_cell_id = %4$s RETURNING geom) 
    INSERT INTO %1$s (geo) SELECT distinct (ST_dump(geom)).geom as geo from long_lines'
  ,_table_name_result_prefix||'_border_line_many_points', _max_point_in_line, ST_ExteriorRing(_bb), cell_id);

    
 
  -- make line part for outer box, that contains the line parts will be added add the final stage when all the cell are done.
 
  EXECUTE Format('INSERT INTO %s (geo, point_geo)
  SELECT l.geom as geo, NULL AS point_geo
  FROM tmp_data_all_lines l where ST_Intersects(geom,%2$L) and min_cell_id = %3$s and ST_IsValid(l.geom) = true '
  ,_table_name_result_prefix||'_border_line_segments', ST_ExteriorRing(_bb), cell_id );

      
  -- return the result of inner geos to handled imediatly
  RETURN QUERY
  SELECT * FROM (
    SELECT l.geom as geo, false as outer_border_line FROM
    tmp_data_all_lines l WHERE  ST_CoveredBy(l.geom,_bb)
    union 
    SELECT ST_Union(out.geom) as geo, true as outer_border_line FROM
    tmp_data_all_lines out where ST_Intersects(out.geom,ST_ExteriorRing(_bb))
 
  ) AS f;
END
$function$;

--drop table if exists test_tmp_simplified_border_lines_1;
--
--create table test_tmp_simplified_border_lines_1 as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('sl_esh.ar50_utvikling_flate','geo',
--'0103000020E9640000010000000500000000000000B0A6074100000000F9F05A4100000000B0A607410000008013F75A4100000000006A08410000008013F75A4100000000006A084100000000F9F05A4100000000B0A6074100000000F9F05A41'
--,'1','test_topo_ar50_t11.ar50_utvikling_flate') g);
--alter table test_tmp_simplified_border_lines_1 add column id serial;
--
--drop if exists table test_tmp_simplified_border_lines_2;
--
--create table test_tmp_simplified_border_lines_2 as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('sl_esh.ar50_utvikling_flate','geo',
--'0103000020E9640000010000000500000000000000B0A60741000000C0EBED5A4100000000B0A6074100000000F9F05A41000000005808084100000000F9F05A410000000058080841000000C0EBED5A4100000000B0A60741000000C0EBED5A41'
--,'1','test_topo_ar50_t11.ar50_utvikling_flate') g);
--
--alter table test_tmp_simplified_border_lines_2 add column id serial;

--drop table if exists test_tmp_simplified_border_lines_2 ;
--
--TRUNCATE topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata_border_line_segments ;
--
--create table test_tmp_simplified_border_lines_2 as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('sl_kbj.trl_2019_test_segmenter_mindredata','geo',
--'0103000020E9640000010000000500000000000000F0BA0D4168C21B5B21B65A4100000000F0BA0D41D850E0F993B85A4100000000FA140E41D850E0F993B85A4100000000FA140E4168C21B5B21B65A4100000000F0BA0D4168C21B5B21B65A41'
--,'1','topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata') g);
--
--alter table test_tmp_simplified_border_lines_2 add column id serial;

--\timing
--drop table if exists test_tmp_simplified_border_lines_1 ;
--
--TRUNCATE topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata_border_line_segments ;
--
--create table test_tmp_simplified_border_lines_1 as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('sl_kbj.trl_2019_test_segmenter_mindredata','geo',
--'0103000020E9640000010000000500000000000000FA140E4154C404F028CC5A4100000000FA140E41C452C98E9BCE5A4100000000046F0E41C452C98E9BCE5A4100000000046F0E4154C404F028CC5A4100000000FA140E4154C404F028CC5A41'
--,'1','topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata') g);
--
--alter table test_tmp_simplified_border_lines_1 add column id serial;
--
--drop table if exists test_tmp_simplified_border_lines_2 ;
--
--create table test_tmp_simplified_border_lines_2 as 
--(select g.* , ST_NPoints(geo) as num_points, ST_IsClosed(geo) as is_closed  
--FROM topo_update.get_simplified_border_lines('sl_kbj.trl_2019_test_segmenter_mindredata','geo',
--'0103000020E9640000010000000500000000000000F0BA0D4154C404F028CC5A4100000000F0BA0D41C452C98E9BCE5A4100000000FA140E41C452C98E9BCE5A4100000000FA140E4154C404F028CC5A4100000000F0BA0D4154C404F028CC5A41'
--,'1','topo_sr16_mdata_05.trl_2019_test_segmenter_mindredata') g);
--
--alter table test_tmp_simplified_border_lines_2 add column id serial;
--
