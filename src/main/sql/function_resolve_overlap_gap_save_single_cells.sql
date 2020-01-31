-- Find cell ready save in master table
CREATE OR REPLACE PROCEDURE resolve_overlap_gap_save_single_cells (_topology_name varchar, _snap_tolerance double precision, _table_name_result_prefix varchar)
LANGUAGE plpgsql
AS $$
DECLARE
  command_string text;
  start_time timestamp WITH time zone;
  done_time timestamp WITH time zone;
  used_time int;
  num_jobs int = 0;
  num_jobs_done int = 0;
  last_done_id int = - 1;
  next_job int = - 1;
  box_id int;
  jobs_done_list varchar = '-1';
  job_list_name varchar = _table_name_result_prefix || '_job_list';
  topo_exist boolean;
  v_state text;
  v_msg text;
  v_detail text;
  v_hint text;
  v_context text;
BEGIN
  -- check total number og jobs to wait for
  command_string := Format('SELECT count(*) from %s as gt', job_list_name);
  EXECUTE command_string INTO num_jobs;
  RAISE NOTICE 'num_jobs is % ', num_jobs;

  LOOP
    --	execute command_string;
    command_string := Format('select gt.id from %s as gt where gt.id not in(%s) order by done_time limit 1', job_list_name || '_donejobs', jobs_done_list);
    EXECUTE command_string INTO next_job;
    IF next_job IS NOT NULL THEN
      last_done_id := next_job;
      num_jobs_done := num_jobs_done + 1;
      box_id := next_job;
      jobs_done_list = jobs_done_list || ',' || next_job::Varchar;
      command_string := Format('SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %L)', _topology_name || '_' || box_id);
      EXECUTE command_string INTO topo_exist;
      IF topo_exist = true THEN
	    BEGIN
          RAISE NOTICE 'Start saving data to cell at timeofday:% for layer %, with box_id % , used % seconds.', Timeofday(), _topology_name, box_id, used_time;
          start_time := Clock_timestamp();
          -- _topology_name character varying, _new_line geometry, _snap_tolerance float, _table_name_result_prefix varchar
          command_string := Format('SELECT topo_update.add_border_lines(%1$L, r.geom, %2$s, %3$L) FROM (
                     SELECT geom from  %4$s.edge) as r', _topology_name, _snap_tolerance, _table_name_result_prefix, _topology_name || '_' || box_id);
          --RAISE NOTICE 'command_string %', command_string;
          EXECUTE command_string;
          used_time := (Extract(EPOCH FROM (Clock_timestamp() - start_time)));
          start_time := Clock_timestamp();
          PERFORM topology.DropTopology (_topology_name || '_' || box_id);
          RAISE NOTICE 'Done saving and deleting data for cell at timeofday:% for layer %, with box_id % , used % seconds.', Timeofday(), _topology_name, box_id, used_time;
          EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_msg = MESSAGE_TEXT, v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT,
            v_context = PG_EXCEPTION_CONTEXT;
          RAISE NOTICE 'Failed handle topology cleaup for % state  : % message: % detail : % hint   : % context: %', _topology_name || '_' || box_id, v_state, v_msg, v_detail, v_hint, v_context;
          END;
      END IF;
      next_job = null;
    ELSE
      RAISE NOTICE 'sleep at to wait nest job to be ready num_jobs_done = %, num_jobs % ', num_jobs_done, num_jobs;
      PERFORM Pg_sleep(1);
    END IF;
    EXIT
    WHEN num_jobs_done = num_jobs;
    RAISE NOTICE ' num_jobs_done = %, num_jobs % ', num_jobs_done, num_jobs;
    
  END LOOP;
END
$$;

--CALL resolve_overlap_gap_save_single_cells('test_topo_jm',0.000001,'test_topo_jm.jm_ukomm_flate_problem');
--CALL resolve_overlap_gap_save_single_cells ('test_topo_ar5', 0.000001, 'test_topo_ar5.ar5_2019_komm_flate');

