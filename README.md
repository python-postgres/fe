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

In v2.0, support for older versions of PostgreSQL and Python will be removed.
If you have automated installations using PyPI, make sure that they specify a major version.

In v1.3, `postgresql.driver.dbapi20.connect` will now raise `ClientCannotConnectError` directly.
Exception traps around connect should still function, but the `__context__` attribute
on the error instance will be `None` in the usual failure case as it is no longer
incorrectly chained. Trapping `ClientCannotConnectError` ahead of `Error` should
allow both cases to co-exist in the event that data is being extracted from
the `ClientCannotConnectError`.

### Installation

Using `pip` and [PyPI](https://PyPI.org):

```bash
python3 -m pip install py-postgresql
```

From [GitHub](https://github.com) using a full clone:

```bash
git clone https://github.com/python-postgres/fe.git
cd fe
python3 ./setup.py install
```

From [GitHub](https://github.com) using a sparse checkout:

```bash
TARGET="$(pwd)/py-packages"
export PYTHONPATH="$PYTHONPATH:$TARGET"
(set -e
	BRANCH=v1.3
	git clone --origin=pypg-frontend --branch=$BRANCH \
		--sparse --filter=blob:none --no-checkout --depth=1 \
		https://github.com/python-postgres/fe.git "$TARGET"
	cd "$TARGET"
	git sparse-checkout set --no-cone postgresql
	git switch $BRANCH
)
unset TARGET
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
