### About

py-postgresql is a Python 3 package providing modules for working with PostgreSQL.
Primarily, a high-level driver for querying databases.

While py-postgresql is still usable for many purposes, asyncpg and PostgREST are
likely more suitable for most applications:

- https://github.com/MagicStack/asyncpg
- https://postgrest.org

py-postgresql, currently, does not have direct support for high-level async
interfaces provided by recent versions of Python. Future versions may change this.

- [Project Future](https://github.com/python-postgres/fe/issues/124)

### Advisory

In v2.0, many, potentially breaking, changes are planned.
If you have automated installations using PyPI, make sure that they specify a major version.

- Support for older versions of PostgreSQL and Python will be removed. This will allow the driver
to defer version parsing fixing (https://github.com/python-postgres/fe/issues/109), and better prepare for future versions.
- The connection establishment strategy will be simplified to only performing one attempt. `sslmode`
parameter should be considered deprecated. v1.4 will provide a new security parameter implying `sslmode=require`. See (https://github.com/python-postgres/fe/issues/122) and (https://github.com/python-postgres/fe/issues/75).
- StoredProcedure will be removed. See (https://github.com/python-postgres/fe/issues/80).

### Installation

From [PyPI](https://PyPI.org) using `pip`:

```bash
python3 -m pip install py-postgresql
```

From [GitHub](https://github.com) using a full clone:

```bash
git clone https://github.com/python-postgres/fe.git
cd fe
python3 ./setup.py install
```

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

```bash
python3 -m postgresql.bin.pg_python -I 'pq://postgres@localhost:5423/postgres'
```

### Documentation

- https://py-postgresql.readthedocs.io
- https://github.com/python-postgres/fe/issues?q=label%3Ahowto

### Related

- https://postgresql.org
- https://python.org
