##
# copyright 2008, pg/python project.
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
import warnings
import tempfile
from contextlib import closing

from . import api as pg_api
from . import configfile
from . import pg_config
from . import exceptions as pg_exc
from . import driver as pg_driver

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
	'superusername' : '-U',
}

class Cluster(pg_api.Cluster):
	"""
	Interface to a PostgreSQL cluster.

	Provides mechanisms to start, stop, restart, kill, drop, and configure a
	cluster(data directory).
	"""
	def get_pid_from_file(self):
		"""
		The current pid from the postmaster.pid file.
		"""
		try:
			with closing(open(os.path.join(self.data_directory, DEFAULT_PID_FILENAME))) as f:
				return int(f.readline())
		except IOError as e:
			if e.errno in (errno.EIO, errno.ENOENT):
				return None

	@property
	def settings(self):
		if getattr(self, '_settings', None) is None:
			self._settings = configfile.ConfigFile(self.pgsql_dot_conf)
		return self._settings

	@property
	def hba_file(self):
		return self.settings.get(
			'hba_file',
			os.path.join(self.data_directory, DEFAULT_HBA_FILENAME)
		)

	def __init__(self,
		data_directory : "path to the data directory",
		pg_config_path : "path to pg_config to use" = 'pg_config',
		pg_config_data : "pg_config data to use; uses _path if None" = None
	):
		self.data_directory = os.path.abspath(data_directory)
		self.pgsql_dot_conf = os.path.join(data_directory, DEFAULT_CONFIG_FILENAME)
		if pg_config_data is None:
			self.config = pg_config.dictionary(pg_config_path)
		else:
			self.config = pg_config_data

		self.postgres_path = os.path.join(self.config['bindir'], 'postmaster')
		if not os.path.exists(self.postgres_path):
			self.postgres_path = os.path.join(self.config['bindir'], 'postgres')
		self.daemon_process = None
		self.last_known_pid = self.get_pid_from_file()

	def __repr__(self):
		return "%s.%s(%r, %r)" %(
			type(self).__module__,
			type(self).__name__,
			self.data_directory,
			self.postgres_path,
		)

	def init(self,
		initdb : "explicitly state the initdb binary to use" = None,
		verbose = False,
		superuserpass = None,
		**kw
	):
		"""
		Create the cluster at the given `data_directory` using the
		provided keyword parameters as options to the command.

		`command_option_map` provides the mapping of keyword arguments
		to command options.
		"""
		# Transform keyword options into command options for the executable.
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
		verbose = (initdb_option_map['verbose'],) if verbose is False else ()
		if superuserpass is not None:
			pass

		if initdb is None:
			initdb = os.path.join(self.config['bindir'], 'initdb')

		cmd = (initdb, '-D', self.data_directory) + verbose + tuple(opts) + extra_args
		p = sp.Popen(
			cmd,
			close_fds = True,
			stdin = sp.PIPE,
			stdout = logfile,
			stderr = sp.PIPE
		)
		p.stdin.close()

		rc = p.wait()
		if rc != 0:
			raise pg_exc.InitDBError(cmd, rc, p.stderr.read())

	def drop(self):
		"""
		Stop the cluster and remove it from the filesystem
		"""
		if self.running():
			self.kill()
		# Really, using rm -rf would be the best, but use this for portability.
		for root, dirs, files in os.walk(self.data_directory, topdown = False):
			for name in files:
				os.remove(os.path.join(root, name))
			for name in dirs:
				os.rmdir(os.path.join(root, name))	
		os.rmdir(self.data_directory)

	def start(self,
		logfile : "Where to send stderr" = sp.PIPE,
		settings : "Mapping of runtime parameters" = None
	):
		"""
		Start the cluster
		"""
		if self.running():
			return None
		cmd = [self.postgres_path, '-D', self.data_directory]
		if settings is not None:
			for k,v in settings:
				cmd.append('--{k}={v}'.format(k=k,v=v))

		p = sp.Popen(
			cmd,
			close_fds = True,
			stdout = logfile,
			stderr = sp.STDOUT,
			stdin = sp.PIPE,
		)
		p.stdin.close()
		self.last_known_pid = p.pid
		self.daemon_process = p

	def stop(self):
		"""
		Stop the cluster gracefully(SIGTERM).

		Does *not* wait for shutdown.
		"""
		pid = self.get_pid_from_file()
		if pid is not None:
			os.kill(pid, signal.SIGTERM)

	def restart(self, timeout = 10):
		"""
		Restart the cluster gracefully.

		This provides a higher level interface to stopping then starting the
		cluster. It will 
		"""
		if self.running():
			self.stop()
			self.wait_until_stopped(timeout = timeout)
		if not self.running():
			raise ClusterError("failed to shutdown cluster")
		self.start()
		self.wait_until_started(timeout = timeout)

	def kill(self):
		"""
		Stop the cluster immediately(SIGKILL).

		Does *not* wait for shutdown.
		"""
		pid = self.get_pid_from_file()
		if pid is not None:
			os.kill(pid, signal.SIGKILL)

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
		pid = self.get_pid_from_file()
		if pid is None:
			return False
		return os.kill(pid, signal.SIG_DFL) == 0

	def ready_for_connections(self):
		"""
		If the daemon is running, and is not in startup mode.

		This only works for clusters configured for TCP/IP connections.
		"""
		if not self.running():
			return False
		d = self.settings.getset((
			'listen_addresses',
			'port',
		))
		if 'listen_addresses' not in d:
			raise ClusterError(
				"postmaster pings can only be made to TCP/IP configurations"
			)

		# Prefer localhost over other addresses.
		addrs = d['listen_addresses'].split(',')
		if 'localhost' in addrs or '*' in addrs:
			host = 'localhost'
		elif '127.0.0.1' in addrs:
			host = '127.0.0.1'
		elif '::1' in addrs:
			host = '::1'

		try:
			pg_driver.connect(
				user = 'ping',
				host = host,
				port = int(d.get('port') or 5432),
				database = 'template1',
			).close()
		except pg_exc.CannotConnectNowError:
			return False
		except pg_exc.Error:
			return True
		except:
			return False

		return True

	def wait_until_started(self,
		timeout : "how long to wait before throwing a timeout exception" = 10,
		delay : "how long to sleep before re-testing" = 0.1
	):
		"""
		After the `start` method is used, this can be ran in order to block until
		the cluster is ready for use.
		"""
		start = time.time()
		while True:
			if not self.running():
				raise pg_exc.ClusterNotRunningError("postres daemon has not been started")

			if self.ready_for_connections():
				return

			if time.time() - start >= timeout:
				raise pg_exc.ClusterTimeoutError(
					"start operation timed out: %d seconds elapsed" %(
						timeout
					)
				)
			time.sleep(delay)

	def wait_until_stopped(self,
		timeout : "how long to wait before throwing a timeout exception" = 10,
		delay : "how long to sleep before re-testing" = 0.1
	):
		"""
		After the `stop` method is used, this can be ran in order to block until
		the cluster is shutdown.

		Additionally, catching `ClusterTimeoutError` exceptions would be a
		starting point for making decisions about whether or not to issue a kill
		to the postgres daemon.
		"""
		start = time.time()
		while self.running():
			if time.time() - start >= timeout:
				raise pg_exc.ClusterTimeoutError(
					"stop operation timed out: %d seconds elapsed" %(
						timeout
					)
				)
			time.sleep(delay)
##
# vim: ts=3:sw=3:noet:
