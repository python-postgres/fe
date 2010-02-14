##
# libsys.sql
##
-- Queries for dealing with the PostgreSQL catalogs for supporting the driver.

[lookup_type::first]
SELECT
 ns.nspname as namespace,
 bt.typname,
 bt.typtype,
 bt.typlen,
 bt.typelem,
 bt.typrelid,
 ae.oid AS ae_typid,
 ae.typreceive::oid != 0 AS ae_hasbin_input,
 ae.typsend::oid != 0 AS ae_hasbin_output
FROM pg_catalog.pg_type bt
 LEFT JOIN pg_type ae
  ON (
   bt.typlen = -1 AND
   bt.typelem != 0 AND
   bt.typelem = ae.oid
  )
 LEFT JOIN pg_catalog.pg_namespace ns
  ON (ns.oid = bt.typnamespace)
WHERE bt.oid = $1

[lookup_composite]
-- Get the type Oid and name of the attributes in `attnum` order.
SELECT
 CAST(atttypid AS oid) AS atttypid,
 CAST(attname AS text) AS attname
FROM
 pg_catalog.pg_type t LEFT JOIN pg_catalog.pg_attribute a
  ON (t.typrelid = a.attrelid)
WHERE
 attrelid = $1 AND NOT attisdropped AND attnum > 0
ORDER BY attnum ASC

[lookup_procedures]
SELECT
 pg_proc.oid,
 pg_proc.*,
 pg_proc.oid::regproc AS _proid,
 pg_proc.oid::regprocedure as procedure_id,
 COALESCE(string_to_array(trim(replace(textin(oidvectorout(proargtypes)), ',', ' '), '{}'), ' ')::oid[], '{}'::oid[])
  AS proargtypes,
 (pg_type.oid = 'record'::regtype or pg_type.typtype = 'c') AS composite
FROM
 pg_catalog.pg_proc LEFT JOIN pg_catalog.pg_type ON (
  pg_proc.prorettype = pg_type.oid
 )

[lookup_procedure_oid::first]
*[lookup_procedures]
 WHERE pg_proc.oid = $1

[lookup_procedure_rp::first]
*[lookup_procedures]
 WHERE pg_proc.oid = regprocedurein($1)

[lookup_prepared_xacts::first]
SELECT
	COALESCE(ARRAY(
		SELECT
			gid::text
		FROM
			pg_catalog.pg_prepared_xacts
		WHERE
			database = current_database()
			AND (
				owner = $1::text
				OR (
					(SELECT rolsuper FROM pg_roles WHERE rolname = $1::text)
				)
			)
		ORDER BY prepared ASC
	), ('{}'::text[]))

[xact_is_prepared::first]
SELECT TRUE FROM pg_catalog.pg_prepared_xacts WHERE gid::text = $1

[get_statement_source::first]
SELECT statement FROM pg_catalog.pg_prepared_statements WHERE name = $1

[setting_get]
SELECT setting FROM pg_catalog.pg_settings WHERE name = $1

[setting_set::first]
SELECT pg_catalog.set_config($1, $2, false)

[setting_len::first]
SELECT count(*) FROM pg_catalog.pg_settings

[setting_item]
SELECT name, setting FROM pg_catalog.pg_settings WHERE name = $1

[setting_mget]
SELECT name, setting FROM pg_catalog.pg_settings WHERE name = ANY ($1)

[setting_keys]
SELECT name FROM pg_catalog.pg_settings ORDER BY name

[setting_values]
SELECT setting FROM pg_catalog.pg_settings ORDER BY name

[setting_items]
SELECT name, setting FROM pg_catalog.pg_settings ORDER BY name

[setting_update]
SELECT
	($1::text[][])[i][1] AS key,
	pg_catalog.set_config(($1::text[][])[i][1], $1[i][2], false) AS value
FROM
	pg_catalog.generate_series(1, array_upper(($1::text[][]), 1)) g(i)

[startup_data:transient:first]
SELECT
 pg_catalog.version()::text,
 backend_start::text,
 client_addr::text,
 client_port
FROM pg_catalog.pg_stat_activity WHERE procpid = pg_catalog.pg_backend_pid();

[startup_data_no_start:transient:first]
SELECT
 pg_catalog.version()::text,
 NULL::text AS backend_start,
 client_addr::text,
 client_port
FROM pg_catalog.pg_stat_activity WHERE procpid = pg_catalog.pg_backend_pid();

[startup_data_only_version:transient:first]
SELECT
 pg_catalog.version()::text,
 NULL::text AS backend_start,
 NULL::text AS client_addr,
 NULL::text AS client_port;

[sizeof_db:transient:first]
SELECT pg_catalog.pg_database_size(current_database())::bigint

[sizeof_cluster:transient:first]
SELECT SUM(pg_catalog.pg_database_size(datname))::bigint FROM pg_database

[sizeof_relation::first]
SELECT pg_catalog.pg_relation_size($1::text)::bigint

[pg_reload_conf:transient:]
SELECT pg_reload_conf()

[languages:transient:column]
SELECT lanname FROM pg_catalog.pg_language
