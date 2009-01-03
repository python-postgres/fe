# -*- encoding: utf-8 -*-
# $Id: test.py,v 1.3 2008/08/17 07:16:21 jwp Exp $
##
# copyright 2007, pg/python project.
# http://python.projects.postgresql.org
##
import unittest
import tempfile
import postgresql.environ as client_environ

service_file = tempfile.mktemp()
sf = open(service_file, 'w')
try:
	sf.write("""
[foo]
user=foo_user
host=foo.com
""")
finally:
	sf.close()

env_samples = [
	(
		{
			'PGUSER' : 'the_user',
			'PGHOST' : 'the_host',
		},
		{
			'user' : 'the_user',
			'host' : 'the_host',
		}
	),
	(
		{
			'PGIRI' : 'pqs://localhost:5432/dbname/ns'
		},
		{
			'sslmode' : 'require',
			'host' : 'localhost',
			'port' : 5432,
			'database' : 'dbname',
			'path' : ['ns']
		}
	),
	(
		{
			'PGIRI' : 'pq://localhost:5432/dbname/ns',
			'PGHOST' : 'override',
		},
		{
			'host' : 'override',
			'port' : 5432,
			'database' : 'dbname',
			'path' : ['ns']
		}
	),
	(
		{
			'PGUNIX' : '/path',
			'PGROLE' : 'myrole',
		},
		{
			'unix' : '/path',
			'role' : 'myrole'
		}
	),
	(
		{
			'PGSERVICEFILE' : service_file,
			'PGSERVICE' : 'foo',
			'PGHOST' : 'unseen',
			'PGPORT' : 5432,
		},
		{
			'user' : 'foo_user',
			'host' : 'foo.com',
			'port' : 5432,
		}
	)
]

class environ(unittest.TestCase):
	def runTest(self):
		for env, dst in env_samples:
			z = client_environ.convert_environ(env)
			self.failUnless(dst == z,
				"environment conversion incongruity %r -> %r != %r" %(
					env, z, dst
				)
			)

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
