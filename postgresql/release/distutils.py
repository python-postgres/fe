##
# .release.distutils - distutils data
##
"""
Python distutils data provisions module.

For sub-packagers, the `prefixed_packages` and `prefixed_extensions` functions
should be of particular interest. If the distribution including ``py-postgresql``
uses the standard layout, chances are that `prefixed_extensions` and
`prefixed_packages` will supply the appropriate information by default as they
use `default_prefix` which is derived from the module's `__package__`.
"""
import sys
import os
from ..project import version, name, identity as url
try:
	from setuptools import Extension, Command
except ImportError as e:
	from distutils.core import Extension, Command

LONG_DESCRIPTION = """
.. warning::
	In v1.3, `postgresql.driver.dbapi20.connect` will now raise `ClientCannotConnectError` directly.
	Exception traps around connect should still function, but the `__context__` attribute
	on the error instance will be `None` in the usual failure case as it is no longer
	incorrectly chained. Trapping `ClientCannotConnectError` ahead of `Error` should
	allow both cases to co-exist in the event that data is being extracted from
	the `ClientCannotConnectError`.

py-postgresql is a set of Python modules providing interfaces to various parts
of PostgreSQL. Primarily, it provides a pure-Python driver with some C optimizations for
querying a PostgreSQL database.

http://github.com/python-postgres/fe

Features:

 * Prepared Statement driven interfaces.
 * Cluster tools for creating and controlling a cluster.
 * Support for most PostgreSQL types: composites, arrays, numeric, lots more.
 * COPY support.

Sample PG-API Code::

	>>> import postgresql
	>>> db = postgresql.open('pq://user:password@host:port/database')
	>>> db.execute("CREATE TABLE emp (emp_first_name text, emp_last_name text, emp_salary numeric)")
	>>> make_emp = db.prepare("INSERT INTO emp VALUES ($1, $2, $3)")
	>>> make_emp("John", "Doe", "75,322")
	>>> with db.xact():
	...  make_emp("Jane", "Doe", "75,322")
	...  make_emp("Edward", "Johnson", "82,744")
	...

There is a DB-API 2.0 module as well::

	postgresql.driver.dbapi20

However, PG-API is recommended as it provides greater utility.

Once installed, try out the ``pg_python`` console script::

	$ python3 -m postgresql.bin.pg_python -h localhost -p port -U theuser -d database_name

If a successful connection is made to the remote host, it will provide a Python
console with the database connection bound to the `db` name.
"""

CLASSIFIERS = [
	'Development Status :: 5 - Production/Stable',
	'Intended Audience :: Developers',
	'License :: OSI Approved :: BSD License',
	'License :: OSI Approved :: MIT License',
	'License :: OSI Approved :: Attribution Assurance License',
	'License :: OSI Approved :: Python Software Foundation License',
	'Natural Language :: English',
	'Operating System :: OS Independent',
	'Programming Language :: Python',
	'Programming Language :: Python :: 3',
	'Topic :: Database',
]

subpackages = [
	'bin',
	'encodings',
	'lib',
	'protocol',
	'driver',
	'test',
	'documentation',
	'python',
	'port',
	'release',
	# Modules imported from other packages.
	'resolved',
	'types',
	'types.io',
]
extensions_data = {
	'port.optimized' : {
		'sources' : [os.path.join('port', '_optimized', 'module.c')],
	},
}

subpackage_data = {
	'lib' : ['*.sql'],
	'documentation' : ['*.txt']
}

try:
	# :)
	if __package__ is not None:
		default_prefix = __package__.split('.')[:-1]
	else:
		default_prefix = __name__.split('.')[:-2]
except NameError:
	default_prefix = ['postgresql']

def prefixed_extensions(
	prefix = default_prefix,
	extensions_data = extensions_data,
) -> [Extension]:
	"""
	Generator producing the `distutils` `Extension` objects.
	"""
	pkg_prefix = '.'.join(prefix) + '.'
	path_prefix = os.path.sep.join(prefix)
	for mod, data in extensions_data.items():
		yield Extension(
			pkg_prefix + mod,
			[os.path.join(path_prefix, src) for src in data['sources']],
			libraries = data.get('libraries', ()),
			optional = True,
		)

def prefixed_packages(
	prefix = default_prefix,
	packages = subpackages,
):
	"""
	Generator producing the standard `package` list prefixed with `prefix`.
	"""
	prefix = '.'.join(prefix)
	yield prefix
	prefix = prefix + '.'
	for pkg in packages:
		yield prefix + pkg

def prefixed_package_data(
	prefix = default_prefix,
	package_data = subpackage_data,
):
	"""
	Generator producing the standard `package` list prefixed with `prefix`.
	"""
	prefix = '.'.join(prefix)
	prefix = prefix + '.'
	for pkg, data in package_data.items():
		yield prefix + pkg, data

def standard_setup_keywords(build_extensions = True, prefix = default_prefix):
	"""
	Used by the py-postgresql distribution.
	"""
	d = {
		'name' : name,
		'version' : version,
		'description' : 'PostgreSQL driver and tools library.',
		'long_description' : LONG_DESCRIPTION,
		'long_description_content_type' : 'text/x-rst',
		'author' : 'James William Pye',
		'author_email' : 'james.pye@gmail.com',
		'maintainer' : 'James William Pye',
		'maintainer_email' : 'james.pye@gmail.com',
		'url' : url,
		'classifiers' : CLASSIFIERS,
		'packages' : list(prefixed_packages(prefix = prefix)),
		'package_data' : dict(prefixed_package_data(prefix = prefix)),
		'cmdclass': dict(test=TestCommand),
		'python_requires': '>=3.7',
	}
	if build_extensions:
		d['ext_modules'] = list(prefixed_extensions(prefix = prefix))
	return d

class TestCommand(Command):
	description = "run tests"

	# List of option tuples: long name, short name (None if no short
	# name), and help string.
	user_options = []

	def initialize_options(self):
		pass

	def finalize_options(self):
		pass

	def run(self):
		import unittest
		unittest.main(module='postgresql.test.testall', argv=('setup.py',))
