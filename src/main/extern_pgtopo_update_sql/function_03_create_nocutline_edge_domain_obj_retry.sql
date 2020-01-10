-- This a function that will be called from the client when user is drawing a line
-- This line will be applied the data in the line layer

-- The result is a set of id's of the new line objects created

  drop FUNCTION if exists 
topo_update.create_nocutline_edge_domain_obj_retry(json_feature text,
  border_topo_info topo_update.input_meta_info,
  server_json_feature text);

-- {
CREATE OR REPLACE FUNCTION
topo_update.create_nocutline_edge_domain_obj_retry(
json_feature text,
border_topo_info topo_update.input_meta_info,
server_json_feature text default null)
RETURNS TABLE(id integer) AS $$
DECLARE


-- holds dynamic sql to be able to use the same code for different
command_string text;

-- holde the computed value for json input reday to use
json_input_structure topo_update.json_input_structure;  

-- holde the computed value for json input reday to use
json_input_structure_tmp topo_update.json_input_structure;  

    counter integer:=0;
    g Geometry;
    inGeom Geometry;
    tolerance real = 10;
    start_tolerance integer = 10;

    start_time timestamp with time zone;
    done_time timestamp with time zone;
    used_time real;
    
    apoint geometry[] DEFAULT '{}';
    
    spltt_line geometry;
    
    failed_to_insert boolean ;
    	feat json;

    
rec record;
BEGIN

	start_time  := clock_timestamp();

	-- TODO totally rewrite this code
--	json_input_structure := topo_update.handle_input_json_props(json_feature::json,server_json_feature::json,border_topo_info.srid);
	feat := json_feature::json;
	json_input_structure.input_geo := ST_GeomFromGeoJSON(feat->>'geometry');

	
	start_tolerance = border_topo_info.snap_tolerance;

	BEGIN

		RAISE NOTICE 'work start with %,  containing % points', json_input_structure.input_geo, ST_NumPoints(json_input_structure.input_geo);


		--test remove reptead points
		inGeom := ST_RemoveRepeatedPoints(json_input_structure.input_geo,start_tolerance); 
		json_input_structure.input_geo := inGeom;

	
		IF ST_NumPoints(json_input_structure.input_geo) < 1000 THEN
--			RAISE NOTICE 'work start, ok :% border_layer_id %, with a line containing % points', start_time, border_topo_info.border_layer_id, ST_NumPoints(json_input_structure.input_geo);
			perform topo_update.create_nocutline_edge_domain_try_one( border_topo_info, json_input_structure);
		ELSE
			RAISE NOTICE 'work start, to big:% border_layer_id %, with a line containing % points', start_time, border_topo_info.border_layer_id, ST_NumPoints(json_input_structure.input_geo);
	
			json_input_structure_tmp := topo_update.handle_input_json_props(json_feature::json,server_json_feature::json,border_topo_info.srid);
			inGeom := json_input_structure.input_geo;
	
	
			LOOP
				counter := counter + 1000;
			    -- some computations
			    IF counter > (ST_NPoints(inGeom)-1) THEN
			        EXIT;  -- exit loop
			    ELSE 
			    	apoint := array_append(apoint, ST_PointN(inGeom,counter));
			    	--ST_PointN(g,counter);
			    END IF;
			    
			END LOOP;
	
			spltt_line := ST_Split(inGeom,ST_Multi(ST_Collect(apoint)));
			
			drop table if exists line_list_tmp;
			create temp table line_list_tmp as (select (ST_Dump(spltt_line)).geom AS line_part);
	 
			FOR rec IN
		      SELECT *
		      FROM   line_list_tmp
		   LOOP
		      RAISE NOTICE 'rec %', ST_Length(rec.line_part);
		      json_input_structure_tmp.input_geo = rec.line_part;
		      perform topo_update.create_nocutline_edge_domain_try_one( border_topo_info, json_input_structure_tmp);
		   END LOOP;		
		END IF;

	done_time  := clock_timestamp();
--	RAISE NOTICE 'work done row :% border_layer_id %, using % sec', done_time, border_topo_info.border_layer_id, (EXTRACT(EPOCH FROM (done_time - start_time)));
  
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'failed ::::::1 %', border_topo_info.border_layer_id;
		counter := 0;

		json_input_structure_tmp := topo_update.handle_input_json_props(json_feature::json,server_json_feature::json,border_topo_info.srid);
		inGeom := json_input_structure.input_geo;
		
--	    BEGIN
--		inGeom := ST_RemoveRepeatedPoints(inGeom,start_tolerance); 
--		json_input_structure_tmp.input_geo := inGeom;
--		perform topo_update.create_nocutline_edge_domain_try_one( border_topo_info, json_input_structure_tmp);
--		return;
--	    EXCEPTION WHEN OTHERS THEN
--	    RAISE NOTICE 'failed with ST_RemoveRepeatedPoints % : %', ST_AsText(inGeom), tolerance;
--	    END;
	    
		
		FOR i IN 1..(ST_NPoints(inGeom)-1)
	    LOOP
	    	border_topo_info.snap_tolerance := start_tolerance;

	        counter:=counter+1;
	        g := ST_MakeLine(ST_PointN(inGeom,i),ST_PointN(inGeom,i+1));
	        perform ST_setSrid(g,border_topo_info.srid);
	        BEGIN
		        
		        
		    json_input_structure_tmp.input_geo = g;
			perform topo_update.create_nocutline_edge_domain_try_one( border_topo_info, json_input_structure_tmp);
			
	      	-- catch EXCEPTION
	    	EXCEPTION WHEN OTHERS THEN
	    		RAISE NOTICE 'failed ::::::2 % num %, border_topo_info.snap_tolerance %', border_topo_info.border_layer_id, i, border_topo_info.snap_tolerance;

	    		tolerance := border_topo_info.snap_tolerance;

	    	   WHILE tolerance > 0 LOOP  
	    	   failed_to_insert := false;
	    	   IF tolerance = 1 THEN
				  tolerance := 0.01;
			   ELSE 
			   	tolerance := tolerance - 1;
	    	   END IF; 

			   IF tolerance < 0 THEN
					tolerance := 0;			   
	    	   END IF;
	        		BEGIN
		        	border_topo_info.snap_tolerance = tolerance ;
					perform topo_update.create_nocutline_edge_domain_try_one( border_topo_info, json_input_structure_tmp);
	        		exit;
	    			EXCEPTION WHEN OTHERS THEN
	        			RAISE NOTICE 'failed with with % : %', ST_AsText(g), tolerance;
	        			failed_to_insert := true;
	    			END;
	    	   END LOOP;
	    	   IF failed_to_insert THEN
	    	   	done_time  := clock_timestamp();
				used_time :=  (EXTRACT(EPOCH FROM (done_time - start_time)));
	        	RAISE NOTICE 'ERROR failed to use %, length: % tolerance : %', ST_AsText(g), ST_length(g), tolerance;
				insert into topo_update.no_cut_line_failed(error_info,geo) 
				values('Failed with exception time used ' || used_time::varchar || ' length ' || ST_length(g), g);
	    	   END IF;
	  		END;
	    END LOOP;

	
	END;

	done_time  := clock_timestamp();
	used_time :=  (EXTRACT(EPOCH FROM (done_time - start_time)));
--	RAISE NOTICE 'work done proc :% border_layer_id %, using % secs, num of % rows', done_time, border_topo_info.border_layer_id, used_time,  ST_NumPoints(json_input_structure.input_geo);

	IF used_time > 10 THEN
		RAISE NOTICE 'very long single line % time with geo for % ', used_time, json_input_structure.input_geo;
		insert into topo_update.long_time_log1(execute_time,info,geo) 
		values(used_time,'long ' || used_time::varchar || ' num points ' || ST_NumPoints(json_input_structure.input_geo), json_input_structure.input_geo);
	END IF;
	
	return;

    
END;
$$ LANGUAGE plpgsql;
--}

