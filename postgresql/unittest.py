##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
TestCase subclasses used by postgresql.test.
"""
import sys
import os
import atexit
import unittest

from . import exceptions as pg_exc
from . import cluster as pg_cluster
from . import installation

from .python.socket import find_available_port

class TestCaseWithCluster(unittest.TestCase):
	"""
	postgresql.driver *interface* tests.
	"""
	def __init__(self, *args, **kw):
		super().__init__(*args, **kw)
		self.installation = installation.default()
		self.cluster_path = \
			'py_unittest_pg_cluster_' \
			+ str(os.getpid()) + getattr(self, 'cluster_path_suffix', '')

		if self.installation is None:
			sys.stderr.write("ERROR: cannot find 'default' pg_config\n")
			sys.stderr.write(
				"HINT: set the PGINSTALLATION environment variable to the `pg_config` path\n"
			)
			sys.exit(1)

		self.cluster = pg_cluster.Cluster(
			self.installation,
			self.cluster_path,
		)
		if self.cluster.initialized():
			self.cluster.drop()

	def configure_cluster(self):
		self.cluster_port = find_available_port()
		if self.cluster_port is None:
			pg_exc.ClusterError(
				'failed to find a port for the test cluster on localhost',
				creator = self.cluster
			).raise_exception()
		self.cluster.settings.update(dict(
			port = str(self.cluster_port),
			max_connections = '6',
			shared_buffers = '24',
			listen_addresses = 'localhost',
			log_destination = 'stderr',
			log_min_messages = 'FATAL',
			silent_mode = 'off',
		))
		# 8.4 turns prepared transactions off by default.
		if self.cluster.installation.version_info >= (8,1):
			self.cluster.settings.update(dict(
				max_prepared_transactions = '3',
			))

	def initialize_database(self):
		c = self.cluster.connection(
			user = 'test',
			database = 'template1',
		)
		with c:
			if c.prepare(
				"select true from pg_catalog.pg_database " \
				"where datname = 'test'"
			).first() is None:
				c.execute('create database test')

	def connection(self, *args, **kw):
		return self.cluster.connection(*args, user = 'test', **kw)

	def run(self, *args, **kw):
		if not self.cluster.initialized():
			self.cluster.encoding = 'utf-8'
			self.cluster.init(
				user = 'test',
				encoding = self.cluster.encoding,
				logfile = None,
			)
			sys.stderr.write('*')
			try:
				atexit.register(self.cluster.drop)
				self.configure_cluster()
				self.cluster.start(logfile = sys.stdout)
				self.cluster.wait_until_started()
				self.initialize_database()
			except Exception:
				self.cluster.drop()
				atexit.unregister(self.cluster.drop)
				raise
		if not self.cluster.running():
			self.cluster.start()
			self.cluster.wait_until_started()

		db = self.connection()
		with db:
			self.db = db
			return super().run(*args, **kw)
			self.db = None
