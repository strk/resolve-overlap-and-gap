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
  next_save_job int = - 1;
  next_createdata_job int = - 1;
  box_id int;
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
  RAISE NOTICE ' starting to handle num_jobs is % ', num_jobs;

  LOOP

       
    -- check for new save jobs
    command_string := Format('UPDATE %1$s 
    SET  start_time_phase_two = now() 
    WHERE id = ( SELECT id FROM %1$s  WHERE start_time_phase_two is null LIMIT 1 FOR UPDATE SKIP LOCKED )
    RETURNING id', job_list_name || '_donejobs');
    EXECUTE command_string INTO next_save_job;
      
    
    
    IF next_save_job IS NULL or next_save_job = 0 THEN 
      RAISE NOTICE ' start to check for new create job with box_id ';

      -- check if more work to do
      command_string := Format('UPDATE %1$s 
      SET  start_time_phase_one = now() 
      WHERE id = ( SELECT id FROM %1$s  WHERE start_time_phase_one is null LIMIT 1 FOR UPDATE SKIP LOCKED )
      RETURNING id', job_list_name);
      EXECUTE command_string INTO next_createdata_job;
      
      IF next_createdata_job IS NOT NULL and next_createdata_job > 0 THEN
            box_id := next_createdata_job;

        RAISE NOTICE ' start to rund create job with box_id = %  ',next_createdata_job;
        command_string := Format('select sql_to_run from %s where id = %s', job_list_name, next_createdata_job);
  	    EXECUTE command_string INTO command_string ;
  	    EXECUTE command_string;
  	    
  	  
  	    command_string := Format('update %s set done_time_phase_one = now() where id = %s', job_list_name, next_createdata_job);
  	    EXECUTE command_string;
      END IF;
    ELSE 
      num_jobs_done := num_jobs_done + 1;
      box_id := next_save_job;
      RAISE NOTICE ' start to handle save job with box_id = %  ', box_id;

      command_string := Format('SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %L)', _topology_name || '_' || box_id);
      EXECUTE command_string INTO topo_exist;
      IF topo_exist = true THEN

	      start_time := Clock_timestamp();
          PERFORM topology.DropTopology (_topology_name || '_' || box_id);
          RAISE NOTICE 'Done saving and deleting data for cell at timeofday:% for layer %, with box_id % , used % seconds.', Timeofday(), _topology_name, box_id, used_time;
      END IF;
      command_string := Format('update %s set done_time_phase_two = now() where id = %s', job_list_name || '_donejobs', next_save_job);
  	  EXECUTE command_string;
    END IF;
    
    command_string := Format('SELECT count(id) from %s as gt where done_time_phase_two is not null', job_list_name|| '_donejobs');
    EXECUTE command_string INTO num_jobs_done;
    RAISE NOTICE ' num_jobs_done = %, num_jobs % ', num_jobs_done, num_jobs;

    COMMIT;

    EXIT
    WHEN num_jobs_done = num_jobs;

    IF next_save_job is null and next_createdata_job is null THEN
      RAISE NOTICE 'sleep at to wait nest job to be ready num_jobs_done = %, num_jobs % ', num_jobs_done, num_jobs;
      PERFORM Pg_sleep(1);
    END IF;

    next_save_job := null;
    next_createdata_job := null;
  END LOOP;
  
  RAISE NOTICE ' done to handle num_jobs is % ', num_jobs;

END
$$;

--CALL resolve_overlap_gap_save_single_cells('test_topo_jm',0.000001,'test_topo_jm.jm_ukomm_flate_problem');
--CALL resolve_overlap_gap_save_single_cells ('test_topo_ar5', 0.000001, 'test_topo_ar5.ar5_2019_komm_flate');

