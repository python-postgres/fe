##
# copyright 2008, pg/python project.
# http://python.projects.postgresql.org
##
"""
Create and interface with PostgreSQL clusters.
"""
import sys
import os
import subprocess as sp
import warnings
from . import api as pg_api
from . import configfile

DEFAULT_CONFIG_FILENAME = 'postgresql.conf'
DEFAULT_HBA_FILENAME = 'pg_hba.conf'

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
	'superuserpass' : '--pwfile',
	'verbose' : '-d',
	'sources' : '-L',
}

class Cluster(pg_api.Cluster):
	"""
	"""
	@property
	def settings(self):
		if not hasattr(self, '_settings'):
			self._settings = configfile.ConfigFile(self.pgsql_dot_conf)
		return self._settings

	@classmethod
	def create(cls,
		path : "path to give to initdb",
		initdb = 'initdb',
		**kw
	):
		"""
		Create the cluster at the given `datadir` using the
		provided keyword parameters as options to the command.

		`command_option_map` provides the mapping of keyword arguments
		to command options.
		"""
		# Transform keyword options into command options for the executable.
		opts = []
		for x in kw:
			if x in ('nowait', 'logfile', 'extra_arguments'):
				continue
			if x not in self.command_option_map:
				raise TypeError("got an unexpected keyword argument %r" %(x,))
			opts.append(self.command_option_map[x])
			opts.append(kw[x])
		nowait = kw.get('nowait', False)
		logfile = kw.get('logfile', sp.PIPE)
		extra_args = kw.get('extra_arguments', ())

		cmd = (self._path, '-D', datadir) + tuple(opts) + extra_args
		p = sp.Popen(
			cmd,
			close_fds = True,
			stdin = sp.PIPE,
			stdout = logfile,
			stderr = sp.PIPE
		)
		p.stdin.close()

		if nowait:
			return p

		try:
			rc = p.wait()
		except KeyboardInterrupt:
			os.kill(p.pid, signal.SIGINT)
			raise

		if rc != 0:
			raise InitDBError(cmd, rc, p.stderr.read())

	def __init__(self, path, postgres_path = 'postgres'):
		"""
		"""
		if not os.path.isdir(path):
			raise ValueError("cluster at %s does not exist" %(path,))
		self.path = path

	def __repr__(self):
		return "%s.%s(%r, %r)" %(
			type(self).__module__,
			type(self).__name__,
			self.path,
			self.postgres_path,
		)

	def drop(self):
		'Stop the cluster and delete it from the filesystem'
		if self.running():
			self.stop()
		# Really, using rm -rf would be the best, but use this for portability.
		for root, dirs, files in os.walk(self.control.data, topdown = False):
			for name in files:
				os.remove(os.path.join(root, name))
			for name in dirs:
				os.rmdir(os.path.join(root, name))	
		os.rmdir(self.control.data)

	def start(self):
		"""
		Start the cluster
		"""
##
# vim: ts=3:sw=3:noet:
