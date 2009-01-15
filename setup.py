#!/usr/bin/env python
##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import sys
import os
from distutils.core import Extension

if sys.version_info[:2] < (3,0):
	sys.stderr.write(
		"ERROR: py-postgresql is for Python 3.0 and greater." + os.linesep
	)
	sys.exit(1)

NAME = 'py-postgresql'
VERSION = '0.8'

LONG_DESCRIPTION = """
py-postgresql is a set of Python modules providing interfaces to various parts
of PostgreSQL. Notably, it provides a driver for interacting with a database.

Sample PG-API Code
------------------

	>>> import postresql.driver as pg_driver
	>>> db = pg_driver.connect(user = 'mydbuser', host = 'localhost', port = 5432, database = 'mydbname')
	>>> db.execute("CREATE TABLE emp (emp_first_name text, emp_last_name text, emp_salary numeric)")
	>>> make_emp = db.query("INSERT INTO emp VALUES ($1, $2, $3)")
	>>> make_emp("John", "Doe", "75,322")
	1
	>>> with db.xact:
	...  make_emp("Jane", "Doe", "75,322")
	...  make_emp("Edward", "Johnson", "82,744")
	...
	1
	1

There is a DB-API 2.0 module as well::

	postgresql.driver.dbapi20

However, PG-API is recommended as it provides greater utility.
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
	'description' : 'Python interfaces to PostgreSQL (driver)',
	'long_description' : LONG_DESCRIPTION,
	'author' : 'James William Pye',
	'author_email' : 'x@jwp.name',
	'maintainer' : 'James William Pye',
	'maintainer_email' : 'python-general@pgfoundry.org',
	'url' : 'http://python.projects.postgresql.org',
	'classifiers' : CLASSIFIERS,

	'packages' : [
		'postgresql',
		'postgresql.encodings',
		'postgresql.protocol',
		'postgresql.driver',
		'postgresql.test',
		# Modules imported from other packages.
		'postgresql.resolved',
	],

	# Only build extension modules on win32 if PY_BUILD_EXTENSIONS is enabled.
	# People who get failures are more likely to just give up on the package
	# without reading the documentation. :(
	'ext_modules' : (
		extensions if sys.platform != 'win32' or os.environ.get('PY_BUILD_EXTENSIONS')
		else ()
	),

	'scripts' : [
		'bin/pg_dotconf',
		'bin/pg_python',
		'bin/pg_tin',
		'bin/pg_withcluster'
	]
}

if __name__ == '__main__':
	from distutils.core import setup
	setup(**defaults)
