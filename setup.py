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
	'classifiers' : CLASSIFIERS,

	'packages' : [
		'postgresql',
		'postgresql.encodings',
		'postgresql.protocol',
		'postgresql.driver',
		'postgresql.test',
	],

	'ext_modules' : [
		Extension(
			'postgresql.protocol.cbuffer',
			[os.path.join('postgresql', 'protocol', 'buffer.c')],
			libraries = (sys.platform == 'win32' and ['ws2_32'] or []),
		),
	],

	'scripts' : ['bin/pg_dotconf', 'bin/pg_python', 'bin/pg_tin']
}

if __name__ == '__main__':
	from distutils.core import setup
	setup(**defaults)
