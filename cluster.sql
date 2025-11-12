\set AUTOCOMMIT on

-- progress table (once)
CREATE SCHEMA IF NOT EXISTS admin;
CREATE TABLE IF NOT EXISTS admin.cluster_progress (
  leaf          regclass PRIMARY KEY,
  clustered_at  timestamptz NOT NULL,
  idx_used      regclass,
  relfilenode   oid
);

CREATE OR REPLACE PROCEDURE admin.cluster_weather()
LANGUAGE plpgsql
AS $$
DECLARE
  v_leaf regclass;
  v_idx  regclass;
BEGIN
  IF NOT pg_try_advisory_lock(8675309) THEN
    RAISE EXCEPTION 'Another run is in progress';
  END IF;

  FOR v_leaf IN
    SELECT t.relid::regclass
    FROM pg_partition_tree('public.weather') AS t
    WHERE t.isleaf
      AND t.parentrelid <> 'public.weather_lat_35'::regclass
      AND NOT EXISTS (
        SELECT 1 FROM admin.cluster_progress p WHERE p.leaf = t.relid::regclass
      )
  LOOP
    BEGIN
      -- find the (lat, lon, date) btree index
      SELECT i.indexrelid::regclass
        INTO v_idx
      FROM pg_index i
      WHERE i.indrelid = v_leaf
        AND pg_get_indexdef(i.indexrelid) ILIKE '%USING btree (lat, lon, date)%'
      LIMIT 1;

      IF v_idx IS NOT NULL AND NOT EXISTS (
           SELECT 1 FROM pg_index x WHERE x.indexrelid = v_idx AND x.indisclustered
         )
      THEN
        EXECUTE format('ALTER TABLE %s SET UNLOGGED', v_leaf);
        EXECUTE format('ALTER TABLE ONLY %s CLUSTER ON %s', v_leaf, v_idx);
        EXECUTE format('CLUSTER VERBOSE %s', v_leaf);
        EXECUTE format('ALTER TABLE %s SET LOGGED', v_leaf);
      END IF;

      INSERT INTO admin.cluster_progress (leaf, clustered_at, idx_used, relfilenode)
      VALUES (
        v_leaf,
        now(),
        v_idx,
        (SELECT c.relfilenode FROM pg_class c WHERE c.oid = v_leaf)
      )
      ON CONFLICT (leaf) DO UPDATE
        SET clustered_at = EXCLUDED.clustered_at,
            idx_used     = EXCLUDED.idx_used,
            relfilenode  = EXCLUDED.relfilenode;

      -- no COMMIT here (we're inside a subtransaction)

    EXCEPTION WHEN OTHERS THEN
      RAISE WARNING 'Failed on %: % (%). Skipping.', v_leaf, SQLERRM, SQLSTATE;
      -- subtransaction is automatically rolled back; proceed
    END;

    -- per-leaf commit (now we're outside the subtransaction)
    COMMIT;
  END LOOP;

  PERFORM pg_advisory_unlock(8675309);
END$$;

CALL admin.cluster_weather();