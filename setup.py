#!/usr/bin/env python
##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import sys
import os
import glob
from distutils.core import Extension

if sys.version_info[:2] < (3,0):
	sys.stderr.write(
		"ERROR: py-postgresql is for Python 3.0 and greater." + os.linesep
	)
	sys.stderr.write(
		"HINT: setup.py was ran using Python " + \
		'.'.join([str(x) for x in sys.version_info[:3]]) + 
		': ' + sys.executable + os.linesep
	)
	sys.exit(1)

sys.path.insert(0, '')
import postgresql
VERSION = postgresql.__version__
NAME = postgresql.__project__
del postgresql
del sys.path[0]

LONG_DESCRIPTION = """
py-postgresql is a set of Python modules providing interfaces to various parts
of PostgreSQL. Notably, it provides a driver for interacting with a database.

Sample PG-API Code
------------------

	>>> import postresql.driver as pg_driver
	>>> db = pg_driver.connect(user = 'mydbuser', host = 'localhost', port = 5432, database = 'mydbname')
	>>> db.execute("CREATE TABLE emp (emp_first_name text, emp_last_name text, emp_salary numeric)")
	>>> make_emp = db.prepare("INSERT INTO emp VALUES ($1, $2, $3)")
	>>> make_emp("John", "Doe", "75,322")
	>>> with db.xact:
	...  make_emp("Jane", "Doe", "75,322")
	...  make_emp("Edward", "Johnson", "82,744")
	...

There is a DB-API 2.0 module as well::

	postgresql.driver.dbapi20

However, PG-API is recommended as it provides greater utility.


History
-------

py-postgresql is not yet another PostgreSQL driver, it's been in development for
years. py-postgresql is the Python 3.0 port of the ``pg_proboscis`` driver and
integration of the other ``pg/python`` projects.


More Information
----------------

http://python.projects.postgresql.org
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

extensions = [
	Extension(
		'postgresql.protocol.cbuffer',
		[os.path.join('postgresql', 'protocol', 'buffer.c')],
		libraries = (sys.platform == 'win32' and ['ws2_32'] or []),
	),
]

defaults = {
	'name' : NAME,
	'version' : VERSION,
	'description' : 'PostgreSQL tool library. Driver, API specifications, and cluster tools.',
	'long_description' : LONG_DESCRIPTION,
	'author' : 'James William Pye',
	'author_email' : 'x@jwp.name',
	'maintainer' : 'James William Pye',
	'maintainer_email' : 'python-general@pgfoundry.org',
	'url' : 'http://python.projects.postgresql.org',
	'classifiers' : CLASSIFIERS,

	'packages' : [
		'postgresql',
		'postgresql.bin',
		'postgresql.encodings',
		'postgresql.protocol',
		'postgresql.driver',
		'postgresql.test',
		# Modules imported from other packages.
		'postgresql.resolved',
		'postgresql.documentation',
		'postgresql.python',
	],

	# Only build extension modules on win32 if PY_BUILD_EXTENSIONS is enabled.
	# People who get failures are more likely to just give up on the package
	# without reading the documentation. :(
	'ext_modules' : (
		extensions if os.environ.get('PY_BUILD_EXTENSIONS') or \
		not sys.platform in ('win32',)
		else ()
	),

	'scripts' : [
		'postgresql/bin/pg_dotconf',
		'postgresql/bin/pg_python',
		'postgresql/bin/pg_tin',
		'postgresql/bin/pg_withcluster'
	],
}

if __name__ == '__main__':
	from distutils.core import setup
	setup(**defaults)
