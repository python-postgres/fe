Changes in v1.2
===============

1.2.2 released on 2020-09-22
----------------------------

 * Correct broken Connection.proc.
 * Correct IPv6 IRI host oversight.
 * Document an ambiguity case of DB-API 2.0 connection creation and the workaround(unix vs host/port).
 * (Pending, active in 1.3) DB-API 2.0 connect() failures caused an undesired exception chain; ClientCannotConnect is now raised.
 * Minor maintenance on tests and support modules.

1.2.0 released on 2016-06-23
----------------------------

 * PostgreSQL 9.3 compatibility fixes (Elvis)
 * Python 3.5 compatibility fixes (Elvis)
 * Add support for JSONB type (Elvis)
