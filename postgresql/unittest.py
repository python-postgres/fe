##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
TestCase subclasses used by postgresql.test.
"""
import sys
import os
import unittest
import atexit

import postgresql.cluster as pg_cluster
import postgresql.installation as pg_inn

TestCluster = pg_cluster.Cluster(
	'py_unittest_postgresql_cluster_' + str(os.getpid()),
	pg_inn.Installation.default()
)
if TestCluster.initialized():
	TestCluster.drop()

class TestCaseWithCluster(unittest.TestCase):
	"""
	postgresql.driver *interface* tests.
	"""
	def run(self, *args, **kw):
		if not TestCluster.initialized():
			TestCluster.init(user = 'test', logfile = sys.stderr)
			try:
				atexit.register(TestCluster.drop)
				TestCluster.settings.update(dict(
					port = '13532', # XXX: identify an available port and use it
					max_connections = '8',
					shared_buffers = '32',
					listen_addresses = 'localhost',
					log_destination = 'stderr',
					log_min_messages = 'FATAL',
				))
				TestCluster.start(logfile = sys.stdout)
				TestCluster.wait_until_started()
				c = TestCluster.connection(
					user = 'test',
					database = 'template1',
				)
				with c:
					if c.prepare(
						"select true from pg_catalog.pg_database " \
						"where datname = 'test'"
					).first() is None:
						c.execute('create database test')
				self.db = c
			except Exception:
				TestCluster.drop()
				atexit.unregister(TestCluster.drop)
				raise
		if not TestCluster.running():
			TestCluster.start()
			TestCluster.wait_until_started()
		if not hasattr(self, 'db'):
			self.db = TestCluster.connection(user = 'test',)
		with self.db:
			return super().run(*args, **kw)
