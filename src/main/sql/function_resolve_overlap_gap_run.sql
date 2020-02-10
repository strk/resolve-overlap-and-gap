-- This is the main funtion used resolve overlap and gap
CREATE OR REPLACE FUNCTION resolve_overlap_gap_run (
-- _table_to_resolve, _table_geo_collumn_name, _table_srid, _utm, 
_table_to_resolve varchar,
_table_geo_collumn_name varchar,
_table_srid int,
_utm boolean,
-- _overlapgap_grid, _table_name_result_prefix, _topology_name, job_list_name,
_overlapgap_grid varchar,
_table_name_result_prefix varchar,
_topology_name varchar, 
_job_list_name varchar,
-- _table_pk_column_name, _simplify_tolerance, _snap_tolerance, _do_chaikins, _min_area_to_keep, _cell_job_type
_table_pk_column_name varchar, 
_simplify_tolerance double precision,
_snap_tolerance double precision,
_do_chaikins boolean,
_min_area_to_keep float,
_cell_job_type int,
_max_parallel_jobs int
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  command_string text;
  -- Holds the list of func_call to run
  stmts text[];
  -- Holds the reseult from paralell calls
  call_result boolean;

BEGIN

  
	    -- 1 ############################# START # add lines inside box and cut lines and save then in separate table,
    -- 2 ############################# START # add border lines saved in last run, we will here connect data from the different cell using he border lines.
    command_string := Format('SELECT resolve_overlap_gap_job_list(%L,%L,%s,%L,%L,%L,%L,%L,%L,%s,%s,%L,%L,%s)', _table_to_resolve, _table_geo_collumn_name, _table_srid, _utm, _overlapgap_grid, _table_name_result_prefix, _topology_name, _job_list_name, _table_pk_column_name, _simplify_tolerance, _snap_tolerance, _do_chaikins, _min_area_to_keep, _cell_job_type);
    --EXECUTE command_string;
    
    
    SELECT ARRAY[command_string] into stmts;
    SELECT execute_parallel (stmts, 1) INTO call_result;
    IF (call_result = FALSE) THEN
      RAISE EXCEPTION 'Failed to run resolve_overlap_gap_job_list for % with the following statement list %', _table_to_resolve, stmts;
    END IF;

    
    LOOP
      stmts := '{}';
      command_string := Format('SELECT ARRAY(SELECT sql_to_run as func_call FROM %s WHERE block_bb is null ORDER BY md5(cell_geo::Text) desc)', _job_list_name);
      RAISE NOTICE 'command_string %', command_string;
      EXECUTE command_string INTO stmts;
      EXIT
      WHEN Array_length(stmts, 1) IS NULL
        OR stmts IS NULL;
      RAISE NOTICE 'array_length(stmts,1) %, stmts %', Array_length(stmts, 1), stmts;
      SELECT execute_parallel (stmts, _max_parallel_jobs) INTO call_result;
      IF (call_result = FALSE) THEN
        RAISE EXCEPTION 'Failed to run overlap and gap for % with the following statement list %', _table_to_resolve, stmts;
      END IF;
    END LOOP;

END
$$;

