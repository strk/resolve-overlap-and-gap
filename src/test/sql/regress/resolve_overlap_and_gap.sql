CREATE EXTENSION dblink; -- needed by  execute_parallel

-- Create data test case degrees
CREATE table test_data.overlap_gap_input_t2 AS (SELECT * from test_data.overlap_gap_input_t1 WHERE c1 in (633,1233,1231,834));

-- Create data test case meter
CREATE table test_data.overlap_gap_input_t3 AS (SELECT distinct c1 as c1t3, c2 as c2t3, c3, ST_transform(geom,25833)::Geometry(Polygon,25833) as geom from test_data.overlap_gap_input_t1 WHERE c1 in (633,1233,1231,834));


-- Call function to resolve overlap and gap in the function in test_data.overlap_gap_input_t1 which we just testet for overlap
CALL resolve_overlap_gap_run('test_data.overlap_gap_input_t2','c1','geom',4258,false,'test_data.overlap_gap_input_t2_res','test_topo_t2',0.000001,5,4);

SELECT 'degrees_check_border_lines', count(geo) from test_topo_t2.border_line_segments;

SELECT 'degrees_check_added_lines', count(geom) from test_topo_t2.edge;

SELECT 'degrees', topology.droptopology('test_topo_t2');

-- Call function to resolve overlap and gap in the function in test_data.overlap_gap_input_t1 which we just testet for overlap
CALL resolve_overlap_gap_run('test_data.overlap_gap_input_t3','c1t3','geom',25833,true,'test_data.overlap_gap_input_t3_res','test_topo_t3',1,5,4);

SELECT 'utm_check_border_lines', count(geo) from test_topo_t3.border_line_segments;

SELECT 'utm_check_added_lines', count(geom) from test_topo_t3.edge;

SELECT 'utm', topology.droptopology('test_topo_t3');


