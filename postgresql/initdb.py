##
# copyright 2008, pg/python project.
# http://python.projects.postgresql.org
##
"""
PostgreSQL catalog cluster initialization interfaces

Python Interface to the ``initdb`` command.
"""
import sys
import os
import subprocess as sp
import signal

class InitializorError(Exception):
	pass

class InitDBError(InitializorError):
	"""
	An initdb InitializorError

	Has the attributes:

	 ``command``
	  The initdb command used.

	 ``result_code``
	  The result code of the process.

	 ``stderr``
	  The error message output.
	"""
	def __init__(self, cmd, rc, err):
		self.command = cmd
		self.result_code = rc
		self.stderr = err

	def __str__(self):
		return ''.join(
			(
				'command failed with non-zero exit code ',
				str(self.result_code),
				os.linesep,
				os.linesep,
				'Command: ',
				' '.join(self.command),
			) + (
				self.stderr and (
					os.linesep,
					'Output:',
					os.linesep,
					'  ' + (os.linesep + '  ').join([
						x for x in self.stderr.splitlines()
					])
				) or ()
			)
		)

class InitDB(Initializor):
	"""
	initdb executable based Initializor implementation
	"""

	command_option_map = {
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

	def sources(self, datadir = None):
		cmd = (self._path, '-s') + \
			datadir is not None and ('-D', datadir, '-s') or ()

		p = sp.Popen(
			cmd,
			stdin = sp.PIPE,
			stdout = sp.PIPE,
			stderr = sp.STDOUT
		)
		p.stdin.close()

		try:
			rc = p.wait()
		except KeyboardInterrupt:
			os.kill(p.pid, signal.SIGINT)
			raise

		if rc != 0:
			raise InitDBError(cmd, rc, p.stdout.read())
		return p.stdout.read()

	def __call__(self, datadir, **kw):
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

def initdb(*cluster_dirs, **kw):
	path = kw.pop('path', None)
	env = kw.pop('environ', os.environ)
	if path is None:
		pgconfig = env.get('PGCONFIG', None)
		if pgconfig is not None:
			path = os.path.join(
				pg_config.dictionary(pgconfig)['bindir'],
				'initdb'
			)
		else:
			path = 'initdb'
	initdbp = InitDB(path)

	for x in cluster_dirs:
		initdbp(x, **kw)
