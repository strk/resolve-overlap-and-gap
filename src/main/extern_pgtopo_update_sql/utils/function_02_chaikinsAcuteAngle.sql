

CREATE OR REPLACE FUNCTION topo_update.chaikinsAcuteAngle (
_geom geometry, 
_max_length int, --max edge length  
_utm boolean, -- utm og degrees coodinates 
_min_degrees int DEFAULT 90, 
_max_degrees int DEFAULT 270, 
_nIterations int DEFAULT 5)
  RETURNS geometry
  AS $$
DECLARE
  --_geom geometry;
  sharp_angle_index int[];
  --dump_point_list geometry_dump[];
  num_points int;
  counter int = 0;
  --_max_length int = 40;
  command_string text;

BEGIN
  -- loop max 5 times, will this ever happen
  -- TODO find a way to 
  IF _utm THEN
  FOR counter IN 1..5 LOOP
    SELECT Array_agg(org_index) INTO sharp_angle_index
    FROM (
      SELECT Abs(Degrees(azimuth_1 - azimuth_2)) AS angle, org_index
        --select 100 as angle, 1 as org_index
      FROM (
        SELECT org_index, ST_Azimuth (p, lead_p) AS azimuth_2, ST_Azimuth (p, lag_p) AS azimuth_1
        FROM (
          SELECT (dp).path[1] AS org_index, Lead((dp).geom) OVER () AS lead_p, (dp).geom AS p, Lag((dp).geom) OVER () AS lag_p
          FROM (
            SELECT ST_DumpPoints (_geom) AS dp) AS r) AS r
        WHERE ST_distance (lead_p, p) < _max_length
          AND ST_distance (p, lag_p) < _max_length) AS r
      WHERE azimuth_1 IS NOT NULL
        AND azimuth_2 IS NOT NULL) AS r
  WHERE angle <= _min_degrees
      OR angle >= _max_degrees;
    -- if there are no sharp angles use return input as it is
    IF sharp_angle_index IS NULL THEN
      EXIT;
    END IF;
    -- get number of points
    num_points := ST_NumPoints (_geom);
    -- assign into varaible
    -- TODO fix this to avoid ST_Dump tywo times
    --SELECT ST_DumpPoints(_geom) into dump_point_list;
    -- get sharp angle	indexes
    -- get new simplfied geom
    _geom := ST_LineFromMultiPoint (mp)
  FROM (
    SELECT ST_Collect (mp) AS mp
    FROM (
      SELECT Unnest(ARRAY[p1, p1_n, p2_n, p2]) AS mp
      FROM (
        SELECT CASE WHEN org_index = 1 THEN
            p1
          ELSE
            NULL
          END AS p1, CASE WHEN p1_n IS NOT NULL THEN
            p1_n
          ELSE
            p1
          END AS p1_n, CASE WHEN p2_n IS NOT NULL THEN
            p2_n
          ELSE
            p2
          END AS p2_n, CASE WHEN org_index = num_points THEN
            p2
          ELSE
            NULL
          END AS p2
        FROM (
          SELECT org_index, p1, CASE WHEN use_p1_n THEN
              ST_LineInterpolatePoint (lp, 0.25)
            ELSE
              NULL
            END AS p1_n,
            CASE WHEN use_p2_n THEN
              ST_LineInterpolatePoint (lp, 0.75)
            ELSE
              NULL
            END AS p2_n, p2
          FROM (
            SELECT r.*, CASE WHEN use_p1_n
                OR use_p2_n THEN
                ST_MakeLine (p1, p2)
              ELSE
                NULL
              END AS lp
            FROM (
              SELECT r.*, CASE WHEN org_index = ANY (sharp_angle_index)
                  OR (org_index - 1) = ANY (sharp_angle_index) THEN
                  TRUE
                ELSE
                  FALSE
                END AS use_p1_n, CASE WHEN org_index = ANY (sharp_angle_index)
                  OR (org_index + 1) = ANY (sharp_angle_index) THEN
                  TRUE
                ELSE
                  FALSE
                END AS use_p2_n
              FROM (
                SELECT (dp).path[1] AS org_index, (dp).geom AS p1, Lead((dp).geom) OVER () AS p2
                FROM (
                  SELECT ST_DumpPoints (_geom) AS dp) AS r) AS r) AS r) AS r) AS r) AS r) AS r) AS r;
    IF counter >= _nIterations THEN
      EXIT;
    END IF;
  END LOOP;
  ELSE
  FOR counter IN 1..5 LOOP
    SELECT Array_agg(org_index) INTO sharp_angle_index
    FROM (
      SELECT Abs(Degrees(azimuth_1 - azimuth_2)) AS angle, org_index
        --select 100 as angle, 1 as org_index
      FROM (
        SELECT org_index, ST_Azimuth (p, lead_p) AS azimuth_2, ST_Azimuth (p, lag_p) AS azimuth_1
        FROM (
          SELECT (dp).path[1] AS org_index, Lead((dp).geom) OVER () AS lead_p, (dp).geom AS p, Lag((dp).geom) OVER () AS lag_p
          FROM (
            SELECT ST_DumpPoints (_geom) AS dp) AS r) AS r
        WHERE ST_distance (lead_p, p, true) < _max_length
          AND ST_distance (p, lag_p,true) < _max_length) AS r
      WHERE azimuth_1 IS NOT NULL
        AND azimuth_2 IS NOT NULL) AS r
  WHERE angle <= _min_degrees
      OR angle >= _max_degrees;
    -- if there are no sharp angles use return input as it is
    IF sharp_angle_index IS NULL THEN
      EXIT;
    END IF;
    -- get number of points
    num_points := ST_NumPoints (_geom);
    -- assign into varaible
    -- TODO fix this to avoid ST_Dump tywo times
    --SELECT ST_DumpPoints(_geom) into dump_point_list;
    -- get sharp angle	indexes
    -- get new simplfied geom
    _geom := ST_LineFromMultiPoint (mp)
  FROM (
    SELECT ST_Collect (mp) AS mp
    FROM (
      SELECT Unnest(ARRAY[p1, p1_n, p2_n, p2]) AS mp
      FROM (
        SELECT CASE WHEN org_index = 1 THEN
            p1
          ELSE
            NULL
          END AS p1, CASE WHEN p1_n IS NOT NULL THEN
            p1_n
          ELSE
            p1
          END AS p1_n, CASE WHEN p2_n IS NOT NULL THEN
            p2_n
          ELSE
            p2
          END AS p2_n, CASE WHEN org_index = num_points THEN
            p2
          ELSE
            NULL
          END AS p2
        FROM (
          SELECT org_index, p1, CASE WHEN use_p1_n THEN
              ST_LineInterpolatePoint (lp, 0.25)
            ELSE
              NULL
            END AS p1_n,
            CASE WHEN use_p2_n THEN
              ST_LineInterpolatePoint (lp, 0.75)
            ELSE
              NULL
            END AS p2_n, p2
          FROM (
            SELECT r.*, CASE WHEN use_p1_n
                OR use_p2_n THEN
                ST_MakeLine (p1, p2)
              ELSE
                NULL
              END AS lp
            FROM (
              SELECT r.*, CASE WHEN org_index = ANY (sharp_angle_index)
                  OR (org_index - 1) = ANY (sharp_angle_index) THEN
                  TRUE
                ELSE
                  FALSE
                END AS use_p1_n, CASE WHEN org_index = ANY (sharp_angle_index)
                  OR (org_index + 1) = ANY (sharp_angle_index) THEN
                  TRUE
                ELSE
                  FALSE
                END AS use_p2_n
              FROM (
                SELECT (dp).path[1] AS org_index, (dp).geom AS p1, Lead((dp).geom) OVER () AS p2
                FROM (
                  SELECT ST_DumpPoints (_geom) AS dp) AS r) AS r) AS r) AS r) AS r) AS r) AS r) AS r;
    IF counter >= _nIterations THEN
      EXIT;
  END IF;
  END LOOP;
  
  END IF;
  
  RETURN _geom;
END;
$$
LANGUAGE plpgsql
IMMUTABLE;

--ok 
--SELECT  topo_update.chaikinsAcuteAngle(ST_ExteriorRing(geo),100000,'t',120,240) FROM test_data.overlap_gap_input_t4 where c1t3 = 1502 ;

--SELECT ST_Numpoints(ST_ExteriorRing(geom)) FROM test_data.overlap_gap_input_t1 where c1 = 1502 ;
  
--SELECT  topo_update.chaikinsAcuteAngle(ST_ExteriorRing(geom),100000,'f',120,240) FROM test_data.overlap_gap_input_t1 where c1 = 1502 ;
  