-- Find cell ready save in master table
CREATE OR REPLACE PROCEDURE resolve_overlap_gap_save_single_cells (
_topology_name varchar, 
_snap_tolerance double precision, 
_table_name_result_prefix varchar,
_cell_job_type int)
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
  job_loop_counter int = 0;
  jd int;
  num_no_jobs_rounds int = 0;

BEGIN
  start_time = clock_timestamp();

  command_string := Format('SELECT count(*) from %s as gt', job_list_name);
  EXECUTE command_string INTO num_jobs;
  RAISE NOTICE ' starting to handle num_jobs is %  at start_time %s cell_job_type %s ', num_jobs, start_time, _cell_job_type;

  
  LOOP


    RAISE NOTICE 'start to check for new save job at % for_cell_job_type % ' , clock_timestamp(), _cell_job_type;

    -- check for new save jobs
    command_string := Format('UPDATE %1$s 
    SET  start_time_phase_two = now() 
    WHERE id = ( SELECT id FROM %1$s  WHERE start_time_phase_two is null LIMIT 1 FOR UPDATE SKIP LOCKED )
    RETURNING id', job_list_name || '_donejobs');
    EXECUTE command_string INTO next_save_job;
    COMMIT;  
    
    
    IF next_save_job IS NULL or next_save_job = 0 THEN 
      RAISE NOTICE 'start to check for new create job at % for_cell_job_type % ' , clock_timestamp(), _cell_job_type;

      -- check if more work to do
      command_string := Format('UPDATE %1$s 
      SET  start_time_phase_one = now() 
      WHERE id = ( SELECT id FROM %1$s  WHERE start_time_phase_one is null and block_bb is null ORDER BY md5(cell_geo::Text) desc LIMIT 1 FOR UPDATE SKIP LOCKED )
      RETURNING id', job_list_name);
      EXECUTE command_string INTO next_createdata_job;
      COMMIT;
      
      IF next_createdata_job IS NOT NULL and next_createdata_job > 0 THEN
            box_id := next_createdata_job;
        num_no_jobs_rounds := 0;
        job_loop_counter := job_loop_counter + 1;

        RAISE NOTICE 'start to run create job with box_id = %  ',next_createdata_job;
        command_string := Format('select sql_to_run from %s where id = %s', job_list_name, next_createdata_job);
  	    EXECUTE command_string INTO command_string ;
  	    EXECUTE command_string;
  	    COMMIT;
  	    command_string := Format('SELECT count(*) FROM %1$s WHERE id = %2$s',
  	    job_list_name || '_donejobs',next_createdata_job);
  	    
        EXECUTE command_string INTO jd;
        IF jd = 1 THEN 
          command_string := Format('update %s set done_time_phase_one = now() where id = %s', 
  	      job_list_name, next_createdata_job);
  	      EXECUTE command_string;
  	      IF _cell_job_type > 1 THEN
  	        command_string := Format('update %s set start_time_phase_two = now(), done_time_phase_two = now() where id = %s', job_list_name || '_donejobs', next_createdata_job);
  	  		EXECUTE command_string;
  	      END IF;
  	    ELSE 
  	      command_string := Format('update %s set start_time_phase_one = null where id = %s', 
  	      job_list_name, next_createdata_job);
  	      EXECUTE command_string;
  	      next_createdata_job := null;
  	    END IF;
  	    COMMIT;
      END IF;
    ELSIF _cell_job_type = 1 THEN
     job_loop_counter := job_loop_counter + 1;
      num_no_jobs_rounds := 0;
      box_id := next_save_job;
      RAISE NOTICE ' start to handle save job with box_id = % and cell_job_type %s', box_id, _cell_job_type;
      IF _cell_job_type = 1 THEN
        command_string := Format('SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = %L)', _topology_name || '_' || box_id);
        EXECUTE command_string INTO topo_exist;
          IF topo_exist = true THEN
          command_string := Format('SELECT topo_update.add_border_lines(%3$L,r.geom,%1$s,%4$L) FROM (
                  SELECT geom from  %2$s.edge) as r', _snap_tolerance, _topology_name || '_' || box_id, _topology_name, _table_name_result_prefix);
          EXECUTE command_string;
          PERFORM topology.DropTopology (_topology_name || '_' || box_id);
          RAISE NOTICE 'Done saving and deleting data for cell at timeofday:% for layer %, with box_id % .', Timeofday(), _topology_name, box_id;
        END IF;  
      END IF;
      command_string := Format('update %s set done_time_phase_two = now() where id = %s', job_list_name || '_donejobs', next_save_job);
  	  EXECUTE command_string;
  	  COMMIT;
    END IF;
    
    command_string := Format('SELECT count(id) from %s as gt where done_time_phase_two is not null', job_list_name|| '_donejobs');
    EXECUTE command_string INTO num_jobs_done;
    
    done_time = clock_timestamp();
    used_time := (Extract(EPOCH FROM (done_time - start_time)));

    
    IF box_id > 0 and MOD(box_id,50) = 0 THEN
       EXECUTE Format('ANALYZE %s.edge_data', _topology_name);
       EXECUTE Format('ANALYZE %s.node', _topology_name);
       EXECUTE Format('ANALYZE %s.face', _topology_name);
       EXECUTE Format('ANALYZE %s.relation', _topology_name);
     END IF;

    RAISE NOTICE 'job_loop_info  job_loop_counter = %, used_time = % , seconds pr loop avg % cell_job_type %s , num_no_jobs_rounds %s ', job_loop_counter, used_time, used_time/job_loop_counter, _cell_job_type,  num_no_jobs_rounds;


    EXIT
    WHEN num_jobs_done >= num_jobs or num_no_jobs_rounds > 2 or job_loop_counter > 400 or used_time > (60*15);

    IF next_save_job is null and next_createdata_job is null THEN
      RAISE NOTICE 'sleep at to wait nest job to be ready num_jobs_done = %, num_jobs %, cell_job_type %', num_jobs_done, num_jobs, _cell_job_type;
      PERFORM pg_sleep(1);
      num_no_jobs_rounds := num_no_jobs_rounds + 1;
    END IF;

    next_save_job := null;
    next_createdata_job := null;
  END LOOP;

  RAISE NOTICE 'final job_loop_info finish at % job_loop_counter = %, used_time = % , seconds pr loop avg % cell_job_type %', 
  done_time, job_loop_counter, used_time, used_time/job_loop_counter, _cell_job_type ;

END
$$;

--CALL resolve_overlap_gap_save_single_cells('test_topo_jm',0.000001,'test_topo_jm.jm_ukomm_flate_problem');
--CALL resolve_overlap_gap_save_single_cells ('test_topo_ar5', 0.000001, 'test_topo_ar5.ar5_2019_komm_flate');

