# About

py-postgresql is a Python 3 package providing modules for working with PostgreSQL.
Primarily, a high-level driver for querying databases.

For a high performance async interface, MagicStack's asyncpg
http://github.com/MagicStack/asyncpg should be considered.

py-postgresql, currently, does not have direct support for high-level async
interfaces provided by recent versions of Python. Future versions may change this.

# Errata

In v1.3, `postgresql.driver.dbapi20.connect` will now raise `ClientCannotConnectError` directly.
Exception traps around connect should still function, but the `__context__` attribute
on the error instance will be `None` in the usual failure case as it is no longer
incorrectly chained. Trapping `ClientCannotConnectError` ahead of `Error` should
allow both cases to co-exist in the event that data is being extracted from
the `ClientCannotConnectError`.

# Installation

Installation *should* be as simple as:

	$ python3 ./setup.py install

Or:

	$ pip install py-postgresql

# Basic Driver Usage

```python
	import postgresql
	db = postgresql.open('pq://user:password@host:port/database')
	get_table = db.prepare("select * from information_schema.tables where table_name = $1")
	for x in get_table("tables"):
		print(x)
	print(get_table.first("tables"))
```

# Documentation

http://py-postgresql.readthedocs.io

# Related

- http://postgresql.org
- http://python.org
