#!/usr/bin/env python
##
# copyright 2008, pg/python project.
# http://python.projects.postgresql.org
##
import sys
import os
from distutils.core import Extension
NAME = 'py-postgresql'
VERSION = '0.1'

LONG_DESCRIPTION = """
py-postgresql is a set of Python modules providing interfaces to various parts
of PostgreSQL. Notably, it provides a driver for interacting with a database.

examples
--------

Run a console with a connection::

 $ pg_python -h localhost -U pgsql -d postgres
 Python 2.5.1 (r251:54863, Dec  8 2007, 09:22:18) 
 [GCC 4.0.1 (Apple Inc. build 5465)] on darwin
 Type "help", "copyright", "credits" or "license" for more information.
 (SavingConsole)
 >>> pg
 <postgresql.driver.tracenull.PGAPI_Connection[pq://pgsql@localhost:5432/postgres] T.0>
 >>>

Make a PG-API connection manually::

 >>> import postgresql.driver.pgapi as driver
 >>> con = driver.Connector(user = 'pgsql', host = 'localhost', port = 5432, database = 'postgres')
 >>> c=con()

Make a DB-API 2.0 connection manually::

 >>> import postgresql.driver.dbapi20 as db
 >>> c=db.connect(user = 'pgsql', host = 'localhost', port = 5432, database = 'postgres')

Notably, DSN strings are *not* directly supported. However, if you want connection
strings, the ``postgresql.iri`` module has means for parsing PQ
RIs and the ``postgresql.dsn`` module has means for parsing DSNs.
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
	'Topic :: Database',
]

defaults = {
	'name' : NAME,
	'version' : VERSION,
	'description' : 'Python interfaces to PostgreSQL (driver)',
	'long_description' : LONG_DESCRIPTION,
	'author' : 'James William Pye',
	'author_email' : 'x@jwp.name',
	'maintainer' : 'pg/python project',
	'maintainer_email' : 'python-general@pgfoundry.org',
	'url' : 'http://python.projects.postgresql.org',
	'zip_safe' : True,
	'install_requires' : [
		'py-termstyle >= 1.0',
		'py-python_command >= 1.0',
		'py-riparse >= 1.0',
	],
	'classifiers' : CLASSIFIERS,

	'namespace_packages' : [
		'postgresql',
	],

	'packages' : [
		'postgresql',
		'postgresql.encodings',
		'postgresql.protocol',
		'postgresql.driver',
		'postgresql.test',
	],

	#'scripts' : ['bin/pg_dotconf', 'bin/pg_python', 'bin/pg_tin']
	'ext_modules' : [
		Extension(
			'postgresql.protocol.cbuffer',
			[os.path.join('postgresql', 'protocol', 'buffer.c')],
			libraries = (sys.platform == 'win32' and ['ws2_32'] or []),
		),
	],
}

if __name__ == '__main__':
	from distutils.core import setup
	setup(**defaults)
