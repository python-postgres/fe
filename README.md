### About

py-postgresql is a Python 3 package providing modules for working with PostgreSQL.
Primarily, a high-level driver for querying databases.

For a high performance async interface, MagicStack's asyncpg
http://github.com/MagicStack/asyncpg should be considered.

py-postgresql, currently, does not have direct support for high-level async
interfaces provided by recent versions of Python. Future versions may change this.

### Advisory

In v1.3, `postgresql.driver.dbapi20.connect` will now raise `ClientCannotConnectError` directly.
Exception traps around connect should still function, but the `__context__` attribute
on the error instance will be `None` in the usual failure case as it is no longer
incorrectly chained. Trapping `ClientCannotConnectError` ahead of `Error` should
allow both cases to co-exist in the event that data is being extracted from
the `ClientCannotConnectError`.

In v2.0, support for older versions of PostgreSQL and Python will be removed.
If you have automated installations using PyPI, make sure that they specify a major version.

### Installation

Using PyPI.org:

	$ pip install py-postgresql

From a clone:

	$ git clone https://github.com/python-postgres/fe.git
	$ cd fe
	$ python3 ./setup.py install # Or use in-place without installation(PYTHONPATH).

### Basic Usage

```python
import postgresql
db = postgresql.open('pq://user:password@host:port/database')

get_table = db.prepare("SELECT * from information_schema.tables WHERE table_name = $1")
print(get_table("tables"))

# Streaming, in a transaction.
with db.xact():
	for x in get_table.rows("tables"):
		print(x)
```

### Documentation

http://py-postgresql.readthedocs.io

### Related

- http://postgresql.org
- http://python.org
