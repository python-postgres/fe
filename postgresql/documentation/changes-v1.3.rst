Changes in v1.3
===============

1.3.0
-----

 * Commit DB-API 2.0 ClientCannotConnect exception correction.
 * Eliminate types-as-documentation annotations.
 * Add Connection.transaction alias for asyncpg consistency.
 * Eliminate multiple inheritance in `postgresql.api` in favor of ABC registration.
 * Add support for PGTEST environment variable (pq-IRI) to improve test performance
   and to aid in cases where the target fixture is already available.
   This should help for testing the driver against servers that are not actually
   postgresql.
