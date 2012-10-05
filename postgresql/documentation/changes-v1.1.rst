Changes in v1.1
===============

1.1.0 in development
--------------------

 * Remove two-phase commit interfaces per deprecation in v1.0.
   For proper two phase commit use, a lock manager must be employed that
   the implementation did nothing to accommodate for.
 * Add support for unpacking anonymous records (Elvis)
 * Support PostgreSQL 9.2 (Elvis)
 * Add column execution method. (jwp)
 * Add one-shot statement interface. Connection.query (jwp)
