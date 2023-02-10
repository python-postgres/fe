### About

py-postgresql is a Python 3 package providing modules for working with PostgreSQL.
Primarily, a high-level driver for querying databases.

While py-postgresql is still usable for many purposes, asyncpg and PostgREST are
likely more suitable for most applications:

	- http://github.com/MagicStack/asyncpg
	- https://postgrest.org/

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

Direct from a sparse checkout:

	export BRANCH=v1.3
	export TARGET="$(pwd)/py-packages"
	export PYTHONPATH="$PYTHONPATH:$TARGET"
	git clone --origin=pypg-frontend --branch=$BRANCH \
		--sparse --filter=blob:none --no-checkout --depth=1 \
		https://github.com/python-postgres/fe.git "$TARGET"
	pushd "$TARGET"
	git sparse-checkout set --no-cone postgresql
	git switch $BRANCH
	popd; unset TARGET BRANCH

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

REPL with connection bound to `db` builtin:

	python3 -m postgresql.bin.pg_python -I 'pq://postgres@localhost:5423/postgres'

### Documentation

- https://py-postgresql.readthedocs.io
- https://github.com/python-postgres/fe/issues?q=label%3Ahowto

### Related

- https://postgresql.org
- https://python.org
