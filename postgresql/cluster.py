##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Create and interface with PostgreSQL clusters.

Primarily, this means starting and stopping the postgres daemon and modifying
the configuration file.
"""
import sys
import os
import errno
import signal
import time
import io
import subprocess as sp
import tempfile

from . import api as pg_api
from . import configfile
from . import installation as pg_inn
from . import exceptions as pg_exc
from . import driver as pg_driver

DEFAULT_CLUSTER_ENCODING = 'utf-8'
DEFAULT_CONFIG_FILENAME = 'postgresql.conf'
DEFAULT_HBA_FILENAME = 'pg_hba.conf'
DEFAULT_PID_FILENAME = 'postmaster.pid'

initdb_option_map = {
	'encoding' : '-E',
	'locale' : '--locale',
	'collate' : '--lc-collate',
	'ctype' : '--lc-ctype',
	'monetary' : '--lc-monetary',
	'numeric' : '--lc-numeric',
	'time' : '--lc-time',
	'authentication' : '-A',
	'user' : '-U',
}

pg_kill = os.kill

class Cluster(pg_api.Cluster):
	"""
	Interface to a PostgreSQL cluster.

	Provides mechanisms to start, stop, restart, kill, drop, and initalize a
	cluster(data directory).

	Cluster does not strive to be consistent with ``pg_ctl``. This is considered
	to be a base class for managing a cluster, and is intended to be extended to
	accommodate for a particular purpose.
	"""
	driver = pg_driver.default
	installation = None
	data_directory = None
	DEFAULT_CLUSTER_ENCODING = DEFAULT_CLUSTER_ENCODING
	DEFAULT_CONFIG_FILENAME = DEFAULT_CONFIG_FILENAME
	DEFAULT_PID_FILENAME = DEFAULT_PID_FILENAME
	DEFAULT_HBA_FILENAME = DEFAULT_HBA_FILENAME

	@property
	def state(self):
		if self.running():
			return 'running'
		return 'not running'

	def _e_metas(self):
		state = self.state
		yield (None, '[' + state + ']')
		if state == 'running':
			yield ('pid', self.state)

	@property
	def daemon_path(self):
		return self.installation.postmaster or self.installation.postgres

	def get_pid_from_file(self):
		"""
		The current pid from the postmaster.pid file.
		"""
		try:
			path = os.path.join(self.data_directory, self.DEFAULT_PID_FILENAME)
			with open(path) as f:
				return int(f.readline())
		except IOError as e:
			if e.errno in (errno.EIO, errno.ENOENT):
				return None

	@property
	def pid(self):
		"""
		If we have the subprocess, use the pid on the object.
		"""
		pid = self.get_pid_from_file()
		if pid is None:
			d = self.daemon_process
			if d is not None:
				return d.pid
		return pid

	@property
	def settings(self):
		if not hasattr(self, '_settings'):
			self._settings = configfile.ConfigFile(self.pgsql_dot_conf)
		return self._settings

	@property
	def hba_file(self):
		return self.settings.get(
			'hba_file',
			os.path.join(self.data_directory, self.DEFAULT_HBA_FILENAME)
		)

	@classmethod
	def from_pg_config_path(type, data_directory, pg_config_path):
		"""
		Create the cluster using the data_directory and the *path* to pg_config
		"""
		return type(data_directory, pg_inn.Installation(pg_config_path))

	def __init__(self,
		data_directory : "path to the data directory",
		installation : pg_inn.Installation,
	):
		self.data_directory = os.path.abspath(data_directory)
		self.installation = installation
		self.pgsql_dot_conf = os.path.join(
			self.data_directory,
			self.DEFAULT_CONFIG_FILENAME
		)
		self.daemon_process = None
		self.daemon_command = None

	def __repr__(self):
		return "%s.%s(%r, %r)" %(
			type(self).__module__,
			type(self).__name__,
			self.data_directory,
			self.installation,
		)

	def __context__(self):
		return self

	def __enter__(self):
		self.start()
		self.wait_until_started()

	def __exit__(self, typ, val, tb):
		self.stop()
		self.wait_until_stopped()
		return typ is None

	def init(self,
		password : \
			"Password to assign to the " \
			"cluster's superuser(`user` keyword)." = None,
		initdb : "[BEWARE] explicitly state the initdb binary to use" = None,
		**kw
	):
		"""
		Create the cluster at the given `data_directory` using the
		provided keyword parameters as options to the command.

		`command_option_map` provides the mapping of keyword arguments
		to command options.
		"""
		if initdb is None:
			initdb = self.installation.initdb
			if initdb is None:
				raise pg_exc.ClusterInitializationError(
					"unable to find `initdb` executable for installation: " + \
					repr(self.installation),
					creator = self
				)

		# Transform keyword options into command options for the executable.
		kw.setdefault('encoding', self.DEFAULT_CLUSTER_ENCODING)
		opts = []
		for x in kw:
			if x in ('logfile', 'extra_arguments'):
				continue
			if x not in initdb_option_map:
				raise TypeError("got an unexpected keyword argument %r" %(x,))
			opts.append(initdb_option_map[x])
			opts.append(kw[x])
		logfile = kw.get('logfile', sp.PIPE)
		extra_args = tuple([
			str(x) for x in kw.get('extra_arguments', ())
		])

		supw_file = ()
		if password is not None:
			# got a superuserpass, store it in a tempfile for initdb
			supw_tmp = tempfile.NamedTemporaryFile(
				mode = 'w', encoding = kw['encoding']
			)
			supw_tmp.write(password)
			supw_tmp.flush()
			supw_file = ('--pwfile=' + supw_tmp.name,)

		cmd = (initdb, '-D', self.data_directory) \
			+ tuple(opts) \
			+ supw_file \
			+ extra_args

		p = sp.Popen(
			cmd,
			stdin = sp.PIPE,
			stdout = logfile,
			stderr = sp.PIPE,
		)
		p.stdin.close()

		while True:
			try:
				rc = p.wait()
				break
			except OSError as e:
				if e.errno != errno.EINTR:
					raise
		if password is not None:
			supw_tmp.close()

		if rc != 0:
			r = p.stderr.read().strip()
			try:
				msg = r.decode('utf-8')
			except UnicodeDecodeError:
				# split up the lines, and use rep.
				msg = os.linesep.join([
					repr(x)[2:-1] for x in r.splitlines()
				])
			raise pg_exc.InitDBError(
				msg,
				details = {
					'COMMAND': cmd,
					'RESULT': rc,
				},
				creator = self
			)

	def drop(self):
		"""
		Stop the cluster and remove it from the filesystem
		"""
		if self.running():
			self.shutdown()
			try:
				self.wait_until_stopped()
			except pg_exc.ClusterTimeoutError:
				self.kill()
				try:
					self.wait_until_stopped()
				except pg_exc.ClusterTimeoutError:
					pg_exc.ClusterWarning(
						'cluster failed to shutdown after kill',
						details = {
							'hint' : 'Shared memory may be leaked.'
						},
						creator = self
					).raise_message()
		# Really, using rm -rf would be the best, but use this for portability.
		for root, dirs, files in os.walk(self.data_directory, topdown = False):
			for name in files:
				os.remove(os.path.join(root, name))
			for name in dirs:
				os.rmdir(os.path.join(root, name))	
		os.rmdir(self.data_directory)

	def start(self,
		logfile : "Where to send stderr" = None,
		settings : "Mapping of runtime parameters" = None
	):
		"""
		Start the cluster.
		"""
		if self.running():
			return
		cmd = (self.daemon_path, '-D', self.data_directory)
		if settings is not None:
			for k,v in dict(settings).items():
				cmd.append('--{k}={v}'.format(k=k,v=v))

		p = sp.Popen(
			cmd,
			stdout = sp.PIPE if logfile is None else logfile,
			stderr = sp.STDOUT,
			stdin = sp.PIPE,
		)
		if logfile is None:
			p.stdout.close()
		p.stdin.close()
		self.daemon_process = p
		self.daemon_command = cmd

	def restart(self, logfile = None, settings = None, timeout = 10):
		"""
		Restart the cluster gracefully.

		This provides a higher level interface to stopping then starting the
		cluster. It will 
		"""
		if self.running():
			self.stop()
			self.wait_until_stopped(timeout = timeout)
		if self.running():
			raise pg_exc.ClusterError(
				"failed to shutdown cluster",
				creator = self
			)
		self.start(logfile = logfile, settings = settings)
		self.wait_until_started(timeout = timeout)

	def reload(self):
		"""
		Signal the cluster to reload its configuration file.
		"""
		pid = self.pid
		if pid is not None:
			try:
				pg_kill(pid, signal.SIGHUP)
			except OSError as e:
				if e.errno != errno.ESRCH:
					raise

	def stop(self):
		"""
		Stop the cluster gracefully waiting for clients to disconnect(SIGTERM).
		"""
		pid = self.pid
		if pid is not None:
			try:
				pg_kill(pid, signal.SIGTERM)
			except OSError as e:
				if e.errno != errno.ESRCH:
					raise

	def shutdown(self):
		"""
		Shutdown the cluster as soon as possible, disconnecting clients.
		"""
		pid = self.pid
		if pid is not None:
			try:
				pg_kill(pid, signal.SIGINT)
			except OSError as e:
				if e.errno != errno.ESRCH:
					raise

	def kill(self):
		"""
		Stop the cluster immediately(SIGKILL).

		Does *not* wait for shutdown.
		"""
		pid = self.pid
		if pid is not None:
			try:
				pg_kill(pid, signal.SIGKILL)
			except OSError as e:
				if e.errno != errno.ESRCH:
					raise
				# already dead, so it would seem.

	def initialized(self):
		"""
		Whether or not the data directory *appears* to be a valid cluster.
		"""
		if os.path.isdir(self.data_directory) and \
		os.path.exists(self.pgsql_dot_conf) and \
		os.path.isdir(os.path.join(self.data_directory, 'base')):
			return True
		return False

	def running(self):
		"""
		Whether or not the postmaster is running.

		This does *not* mean the cluster is accepting connections.
		"""
		if self.daemon_process is not None:
			r = self.daemon_process.poll()
			if r is not None:
				pid = self.get_pid_from_file()
				if pid is not None:
					# daemon process does not exist, but there's a pidfile.
					self.daemon_process = None
					return self.running()
				return False
			else:
				return True
		else:
			pid = self.get_pid_from_file()
			if pid is None:
				return False
			try:
				pg_kill(pid, signal.SIG_DFL)
			except OSError as e:
				if e.errno != errno.ESRCH:
					raise
				return False
			return True

	def connector(self, **kw):
		"""
		Create a postgresql.driver connector based on the given keywords and
		listen_addresses and port configuration in settings.
		"""
		host, port = self.address()
		return self.driver.fit(
			host = host or 'localhost',
			port = port or 5432,
			**kw
		)

	def connection(self, **kw):
		"""
		Create a connection object to the cluster, but do not connect.
		"""
		return self.connector(**kw)()

	def connect(self, **kw):
		"""
		Create an established connection from the connector.

		Cluster must be running.
		"""
		if not self.running():
			raise ClusterNotRunningError(
				"cannot connect if cluster is not running",
				creator = self
			)
		x = self.connection(**kw)
		x.connect()
		return x

	def address(self):
		"""
		Get the host-port pair from the configuration.
		"""
		d = self.settings.getset((
			'listen_addresses', 'port',
		))
		if 'listen_addresses' in d:
			# Prefer localhost over other addresses.
			# More likely to get a successful connection.
			addrs = d.get('listen_addresses', 'localhost').lower().split(',')
			if 'localhost' in addrs or '*' in addrs:
				host = 'localhost'
			elif '127.0.0.1' in addrs:
				host = '127.0.0.1'
			elif '::1' in addrs:
				host = '::1'
			else:
				host = addrs[0]
		else:
			host = None
		return (host, d.get('port'))

	def ready_for_connections(self):
		"""
		If the daemon is running, and is not in startup mode.

		This only works for clusters configured for TCP/IP connections.
		"""
		if not self.running():
			return False
		e = None
		host, port = self.address()
		try:
			self.driver.connect(
				user = ' -*- ping -*- ',
				host = host or 'localhost',
				port = port or 5432,
				database = 'template1',
				sslmode = 'disable',
			).close()
		except pg_exc.ClientCannotConnectError as err:
			for attempt in err.database.failures:
				x = attempt.error
				if self.installation.version_info[:2] < (8,1):
					if isinstance(x, (
						pg_exc.UndefinedObjectError,
						pg_exc.AuthenticationSpecificationError,
					)):
						# undefined user.. whatever...
						return True
				else:
					if isinstance(x, pg_exc.AuthenticationSpecificationError):
						return True
				# configuration file error. ya, that's probably not going to change.
				if isinstance(x, (pg_exc.CFError, pg_exc.ProtocolError)):
					raise x
				if isinstance(x, pg_exc.ServerNotReadyError):
					e = x
					break
			else:
				e = err
		# the else true means we successfully connected with those
		# credentials... strange, but true..
		return e if e is not None else True

	def wait_until_started(self,
		timeout : "how long to wait before throwing a timeout exception" = 10,
		delay : "how long to sleep before re-testing" = 0.05,
	):
		"""
		After the `start` method is used, this can be ran in order to block
		until the cluster is ready for use.

		This method loops until `ready_for_connections` returns `True` in
		order to make sure that the cluster is actually up.
		"""
		start = time.time()
		checkpoint = start
		while True:
			if not self.running():
				if self.daemon_process is not None:
					r = self.daemon_process.returncode
					if r is not None and r != 0:
						raise pg_exc.ClusterStartupError(
							"postgresql daemon exited with non-zero status",
							details = {
								'RESULT' : r,
								'COMMAND' : self.daemon_command,
							},
							creator = self
						)
				else:
					raise pg_exc.ClusterNotRunningError(
						"postgresql daemon has not been started",
						creator = self
					)
			r = self.ready_for_connections()

			checkpoint = time.time()
			if r is True:
				break

			if checkpoint - start >= timeout:
				# timeout was reached, but raise ServerNotReadyError
				# to signal to the user that it was *not* due to some unknown
				# condition, rather it's *still* starting up.
				if r is not None and isinstance(r, pg_exc.ServerNotReadyError):
					raise r
				e = pg_exc.ClusterTimeoutError(
					'timeout on startup',
					creator = self
				)
				if r not in (True,False):
					raise e from r
				raise e
			time.sleep(delay)

	def wait_until_stopped(self,
		timeout : "how long to wait before throwing a timeout exception" = 10,
		delay : "how long to sleep before re-testing" = 0.05
	):
		"""
		After the `stop` method is used, this can be ran in order to block until
		the cluster is shutdown.

		Additionally, catching `ClusterTimeoutError` exceptions would be a
		starting point for making decisions about whether or not to issue a kill
		to the daemon.
		"""
		start = time.time()
		while self.running() is True:
			# pickup the exit code.
			if self.daemon_process is not None:
				self.last_exit_code = self.daemon_process.poll()
			if time.time() - start >= timeout:
				raise pg_exc.ClusterTimeoutError(
					'timeout on shutdown',
					creator = self,
				)
			time.sleep(delay)
##
# vim: ts=3:sw=3:noet:
