##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
'pg_config Python interface; provides member based access to pg_config data'
import sys
import os
import os.path
import subprocess
import io
import errno
from itertools import chain
from operator import itemgetter
from . import versionstring
from . import api as pg_api
from . import string as pg_str
from . import exceptions as pg_exc

def get_command_output(exe, *args):
	'helper function for the instance class'
	pa = [
		'--' + x.strip() for x in args if x is not None
	]
	pa.insert(0, exe)
	p = subprocess.Popen(pa,
		close_fds = True,
		stdout = subprocess.PIPE,
		stderr = subprocess.PIPE,
		stdin = subprocess.PIPE,
		shell = False
	)
	p.stdin.close()
	##
	# Sigh. Fucking gdb.
	while True:
		try:
			rv = p.wait()
			break
		except OSError as e:
			if e.errno != errno.EINTR:
				raise
	if rv != 0:
		return None
	return io.TextIOWrapper(p.stdout).read()

def pg_config_dictionary(pg_config_path):
	"""
	Create a dictionary of the information available in the given
	pg_config_path. This provides a one-shot solution to fetching information
	from the pg_config binary.
	"""
	default_output = get_command_output(pg_config_path)
	if default_output is not None:
		d = {}
		for x in default_output.splitlines():
			if not x or x.isspace() or x.find('=') == -1:
				continue
			k, v = x.split('=', 1)
			# keep it semi-consistent with instance
			d[k.lower().strip()] = v.strip()
		return d

	# Support for 8.0 pg_config and earlier.
	# This requires three invocations of pg_config:
	#  First --help, to get the -- options available,
	#  Second, all the -- options except version.
	#  Third, --version as it appears to be exclusive.
	opt = []
	for l in get_command_output(pg_config_path, 'help').splitlines():
		dash_pos = l.find('--')
		if dash_pos == -1:
			continue
		sp_pos = l.find(' ', dash_pos)
		# the dashes are added by the call command
		opt.append(l[dash_pos+2:sp_pos])
	if 'help' in opt:
		opt.remove('help')
	if 'version' in opt:
		opt.remove('version')

	d=dict(zip(opt, get_command_output(pg_config_path, *opt).splitlines()))
	d['version'] = get_command_output(pg_config_path, 'version').strip()
	return d

def parse_configure_options(confopt):
	quote = confopt[0]
	parts = pg_str.split_using(confopt, quote, sep = ' ')
	for x in parts:
		# remove the quotes around '--' from option
		x = x[1:-1].lstrip('-')
		# if the split fails, the '1' index will
		# be true, indicating that the flag was given.
		kv = x.split('=', 1) + [True]
		yield (kv[0].replace('-','_'), kv[1])

def platform_binary(path):
	return path

if sys.platform == 'win32':
	def platform_binary(path):
		return path + '.exe'

installations = {}
class Installation(pg_api.Installation):
	"""
	Class providing a Python interface to PostgreSQL installation metadata.
	"""
	version = None
	version_info = None
	type = None
	configure_options = None

	pg_binaries = (
		'pg_config',
		'psql',
		'initdb',
		'pg_resetxlog',
		'pg_controldata',
		'clusterdb',
		'pg_ctl',
		'pg_dump',
		'pg_dumpall',
		'postgres',
		'postmaster',
		'reindexdb',
		'vacuumdb',
		'ipcclean',
		'createdb',
		'ecpg',
		'createuser',
		'createlang',
		'droplang',
		'dropuser',
		'pg_restore',
	)

	pg_libraries = (
		'libpq',
		'libecpg',
		'libpgtypes',
		'libecpg_compat',
	)

	pg_config_directories = (
		'bindir',
		'docdir',
		'includedir',
		'pkgincludedir',
		'includedir_server',
		'libdir',
		'pkglibdir',
		'localedir',
		'mandir',
		'sharedir',
		'sysconfdir',
	)

	def _e_metas(self):
		l = list(self.configure_options.items())
		l.sort(key = itemgetter(0))
		yield ('version', self.version)
		if l:
			yield ('configure_options',
				(os.linesep).join((
					k if v is True else k + '=' + v
					for k,v in l
				))
			)

	def __repr__(self):
		return "{mod}.{name}({path!r})".format(
			mod = type(self).__module__,
			name = type(self).__name__,
			path = self.pg_config_path
		)

	@staticmethod
	def default_pg_config():
		pg_config_path = os.environ.get(
			'PGINSTALLATION', ([
				os.path.join(x, 'pg_config')
				for x in os.environ.get('PATH', '').split(os.pathsep)
				if os.path.exists(os.path.join(x, 'pg_config'))
			] + [None])[0]
		)
		return pg_config_path if (
			pg_config_path and os.path.exists(pg_config_path)
		) else None

	@classmethod
	def default(typ):
		path = typ.default_pg_config()
		if path is None:
			return None
		return typ(path)

	def __new__(typ, pg_config_path):
		# One instance for each installation.
		global installations
		if not os.path.isabs(pg_config_path):
			raise ValueError("pg_config path is not absolute, {0!r}".format(
				pg_config_path
			))
		pg_config_path = os.path.realpath(pg_config_path)
		current = installations.get(pg_config_path)
		if current is not None:
			pg_config_data = pg_config_dictionary(pg_config_path)
			if current.version == pg_config_data['version']:
				return current
		else:
			pg_config_data = pg_config_dictionary(pg_config_path)
		rob = super().__new__(typ)
		rob.pg_config_data = pg_config_data
		return rob

	def __init__(self, pg_config_path):
		self.version = self.pg_config_data["version"]
		self.type, vs = self.version.split()
		self.version_info = versionstring.normalize(versionstring.split(vs))
		self.configure_options = dict(
			parse_configure_options(self.pg_config_data.get('configure'))
		)

		bindir_path = self.pg_config_data.get('bindir')
		libdir_path = self.pg_config_data.get('libdir')
		self.paths = {
			k : v for k,v in (
				(k, (v if os.path.exists(v) else None)) for k,v in chain(
					(
						(k, platform_binary(os.path.join(bindir_path, k)))
						for k in self.pg_binaries
					),
					(
						(k, self.pg_config_data[k]) for k in
						self.pg_config_directories if k in self.pg_config_data
					),
				)
			)
		}
		ppg_config = self.paths.get('pg_config')
		if ppg_config is not None:
			self.pg_config_path = ppg_config
		else:
			self.pg_config_path = pg_config_path
		installations[self.pg_config_path] = self

	def __dir__(self):
		return list(set(chain(
			self.paths.keys(),
			dir(super()),
		)))

	@property
	def ssl(self):
		return 'with_openssl' in self.configure_options

	def __getattr__(self, attname):
		try:
			return getattr(super(), attname)
		except AttributeError:
			if attname in self.paths:
				return self.paths[attname]
			raise

if __name__ == '__main__':
	i = Installation(pg_config_path = sys.argv[1])
	from .python.element import format_element
	print(format_element(i))
