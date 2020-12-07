##
# .test.test_cluster
##
import sys
import os
import time
import unittest
import tempfile
from .. import installation
from ..cluster import Cluster, ClusterStartupError

default_installation = installation.default()

class test_cluster(unittest.TestCase):
	def setUp(self):
		self.cluster = Cluster(default_installation, 'test_cluster',)

	def tearDown(self):
		if self.cluster.installation is not None:
			self.cluster.drop()
			self.cluster = None

	def start_cluster(self, logfile = None):
		self.cluster.start(logfile = logfile)
		self.cluster.wait_until_started(timeout = 10)

	def init(self, *args, **kw):
		self.cluster.init(*args, **kw)

		vi = self.cluster.installation.version_info[:2]
		if vi >= (9, 3):
			usd = 'unix_socket_directories'
		else:
			usd = 'unix_socket_directory'

		if vi > (9, 6):
			self.cluster.settings['max_wal_senders'] = '0'

		self.cluster.settings.update({
			'max_connections' : '8',
			'listen_addresses' : 'localhost',
			'port' : '6543',
			usd : self.cluster.data_directory,
		})

	@unittest.skipIf(default_installation is None, "no installation provided by environment")
	def testSilentMode(self):
		self.init()
		self.cluster.settings['silent_mode'] = 'on'
		# if it fails to start(ClusterError), silent_mode is not working properly.
		try:
			self.start_cluster(logfile = sys.stdout)
		except ClusterStartupError:
			# silent_mode is not supported on windows by PG.
			if sys.platform in ('win32','win64'):
				pass
			elif self.cluster.installation.version_info[:2] >= (9, 2):
				pass
			else:
				raise
		else:
			if sys.platform in ('win32','win64'):
				self.fail("silent_mode unexpectedly supported on windows")
			elif self.cluster.installation.version_info[:2] >= (9, 2):
				self.fail("silent_mode unexpectedly supported on PostgreSQL >=9.2")

	@unittest.skipIf(default_installation is None, "no installation provided by environment")
	def testSuperPassword(self):
		self.init(
			user = 'test',
			password = 'secret',
			logfile = sys.stdout,
		)
		self.start_cluster()
		c = self.cluster.connection(
			user='test',
			password='secret',
			database='template1',
		)
		with c:
			self.assertEqual(c.prepare('select 1').first(), 1)

	@unittest.skipIf(default_installation is None, "no installation provided by environment")
	def testNoParameters(self):
		"""
		Simple init and drop.
		"""
		self.init()
		self.start_cluster()

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
