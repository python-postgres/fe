##
# .temporal - manage the temporary cluster
##
"""
Temporary PostgreSQL cluster for the process.
"""
import os
import atexit
import builtins
from collections import deque
from .cluster import Cluster, ClusterError
from . import installation
from .python.socket import find_available_port

class Temporal(object):
	"""
	Manages a temporary cluster for the duration of the process.

	Instances of this class reference a distinct cluster. These clusters are
	transient; they will only exist until the process exits.

	Usage::

		>>> from postgresql.temporal import pg_tmp
		>>> with pg_tmp:
		...  ps = db.prepare('SELECT 1')
		...  assert ps.first() == 1

	Or `pg_tmp` can decorate a method or function.
	"""

	format_sandbox_id = staticmethod(('sandbox{0}_{1}').format)
	cluster_dirname = staticmethod(('pg_tmp_{0}_{1}').format)
	cluster = None

	_init_pid_ = None
	_local_id_ = 0
	builtins_keys = {
		'connector',
		'db',
		'do',
		'xact',
		'proc',
		'settings',
		'prepare',
		'sqlexec',
		'newdb',
	}

	def __init__(self):
		self.builtins_stack = deque()
		self.sandbox_id = 0
		# identifier for keeping temporary instances unique.
		self.__class__._local_id_ = self.local_id = (self.__class__._local_id_ + 1)

	def __call__(self, callable):
		def in_pg_temporal_context(*args, **kw):
			with self:
				return callable(*args, **kw)
		n = getattr(callable, '__name__', None)
		if n:
			in_pg_temporal_context.__name__ = n
		return in_pg_temporal_context

	def destroy(self):
		# Don't destroy if it's not the initializing process.
		if os.getpid() == self._init_pid_:
			# Kill all the open connections.
			try:
				c = cluster.connection(user = 'test', database = 'template1',)
				with c:
					if c.version_info[:2] <= (9,1):
						c.sys.terminate_backends()
					else:
						c.sys.terminate_backends_92()
			except Exception:
				# Doesn't matter much if it fails.
				pass
			cluster = self.cluster
			self.cluster = None
			self._init_pid_ = None
			if cluster is not None:
				cluster.stop()
				cluster.wait_until_stopped(timeout = 5)
				cluster.drop()

	def init(self,
		installation_factory = installation.default,
		inshint = {
			'hint' : "Try setting the PGINSTALLATION " \
			"environment variable to the `pg_config` path"
		}
	):
		if self.cluster is not None or 'PGTEST' in os.environ:
			return
		##
		# Hasn't been created yet, but doesn't matter.
		# On exit, obliterate the cluster directory.
		self._init_pid_ = os.getpid()
		atexit.register(self.destroy)

		# [$HOME|.]/.pg_tmpdb_{pid}
		self.cluster_path = os.path.join(
			os.environ.get('HOME', os.getcwd()),
			self.cluster_dirname(self._init_pid_, self.local_id)
		)
		self.logfile = os.path.join(self.cluster_path, 'logfile')
		installation = installation_factory()
		if installation is None:
			raise ClusterError(
				'could not find the default pg_config', details = inshint
			)

		vi = installation.version_info
		cluster = Cluster(installation, self.cluster_path,)

		# If it exists already, destroy it.
		if cluster.initialized():
			cluster.drop()
		cluster.encoding = 'utf-8'
		cluster.init(
			user = 'test', # Consistent username.
			encoding = cluster.encoding,
			logfile = None,
		)

		try:
			self.cluster_port = find_available_port()
		except:
			# Rely on chain.
			raise ClusterError(
				'could not find a port for the test cluster on localhost',
				creator = cluster
			)

		if vi[:2] > (9,6):
			# Default changed in 10.x
			cluster.settings['max_wal_senders'] = '0'

		cluster.settings.update(dict(
			port = str(self.cluster_port),
			max_connections = '20',
			shared_buffers = '200',
			listen_addresses = 'localhost',
			log_destination = 'stderr',
			log_min_messages = 'FATAL',
			max_prepared_transactions = '10',
		))

		if installation.version_info[:2] < (9, 3):
			cluster.settings.update(dict(
				unix_socket_directory = cluster.data_directory,
			))
		else:
			cluster.settings.update(dict(
				unix_socket_directories = cluster.data_directory,
			))

		# Start the database cluster.
		with open(self.logfile, 'w') as lfo:
			cluster.start(logfile = lfo)
		cluster.wait_until_started()

		# Initialize template1 and the test user database.
		c = cluster.connection(user = 'test', database = 'template1',)
		with c:
			c.execute('create database test')
		self.cluster = cluster

	def push(self):
		if 'PGTEST' in os.environ:
			from . import open as pg_open
			c = pg_open(os.environ['PGTEST']) # Ignoring PGINSTALLATION.
		else:
			c = self.cluster.connection(user = 'test')
			c.connect()

		extras = []
		sbid = self.format_sandbox_id(os.getpid(), self.sandbox_id + 1)

		def new_pg_tmp_connection(l = extras, clone = c.clone, sbid = sbid):
			# Used to create a new connection that will be closed
			# when the context stack is popped along with 'db'.
			l.append(clone())
			l[-1].settings['search_path'] = str(sbid) + ',' + l[-1].settings['search_path']
			return l[-1]

		# The new builtins.
		local_builtins = {
			'db' : c,
			'prepare' : c.prepare,
			'xact' : c.xact,
			'sqlexec' : c.execute,
			'do' : c.do,
			'settings' : c.settings,
			'proc' : c.proc,
			'connector' : c.connector,
			'new' : new_pg_tmp_connection,
		}
		if not self.builtins_stack:
			# Store any of those set or not set.
			current = {
				k : builtins.__dict__[k] for k in self.builtins_keys
				if k in builtins.__dict__
			}
			self.builtins_stack.append((current, []))

		# Store and push.
		self.builtins_stack.append((local_builtins, extras))
		builtins.__dict__.update(local_builtins)
		self.sandbox_id += 1

	def pop(self, exc, drop_schema = ('DROP SCHEMA {0} CASCADE').format):
		local_builtins, extras = self.builtins_stack.pop()
		self.sandbox_id -= 1

		# restore __builtins__
		if len(self.builtins_stack) > 1:
			builtins.__dict__.update(self.builtins_stack[-1][0])
		else:
			previous = self.builtins_stack.popleft()
			for x in self.builtins_keys:
				if x in previous:
					builtins.__dict__[x] = previous[x]
				else:
					# Wasn't set before.
					builtins.__dict__.pop(x, None)

		# close popped connection, but only if we're not in an interrupt.
		# However, temporal will always terminate all backends atexit.
		if exc is None or isinstance(exc, Exception):
			# Interrupt then close. Just in case something is lingering.
			for xdb in [local_builtins['db']] + list(extras):
				if xdb.closed is False:
					# In order for a clean close of the connection,
					# interrupt before closing. It is still
					# possible for the close to block, but less likely.
					xdb.interrupt()
				xdb.close()

			# Interrupted and closed all the other connections at this level;
			# now remove the sandbox schema.
			xdb = local_builtins['db']
			with xdb.clone() as c:
				# Use a new connection so that the state of
				# the context connection will not have to be
				# contended with.
				c.execute(drop_schema(self.format_sandbox_id(os.getpid(), self.sandbox_id + 1)))
		else:
			# interrupt exception; avoid waiting for close
			pass

	def _init_c(self, cxn):
		cxn.connect()
		sb = self.format_sandbox_id(os.getpid(), self.sandbox_id)
		cxn.execute('CREATE SCHEMA ' + sb)
		cxn.settings['search_path'] = ','.join((sb, cxn.settings['search_path']))

	def __enter__(self):
		if self.cluster is None:
			self.init()

		self.push()
		try:
			self._init_c(builtins.db)
		except Exception as e:
			# failed to initialize sandbox schema; pop it.
			self.pop(e)
			raise

	def __exit__(self, exc, val, tb):
		self.pop(val)

#: The process' temporary cluster or connection source.
pg_tmp = Temporal()
