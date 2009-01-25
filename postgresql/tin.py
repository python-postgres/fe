##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
tin provides a means to manage a set of test clusters.
"""
import sys
import os
import signal
import traceback
import subprocess
import warnings
from random import random

from . import versionstring as pg_version
from . import pg_config
from .cluster import Cluster

from gettext import gettext as _

import optparse

# FIXME: This should check if it can bind the port as well.
def genport(exceptions = (), limit = 100):
	port = 0
	exceptions = (port,) + tuple(exceptions)
	while port in exceptions and limit:
		limit -= 1
		# Get a random a port from 5433 to 55433
		port = int(random() * 50000) + 5433
	return port

class Cup(object):
	'The cup of clusters.'
	def __init__(self, path, parallels = 0):
		self.path = path
		if not os.path.exists(path):
			os.mkdir(path)

		clusters = []
		for x in os.listdir(path):
			clpath = os.path.join(path, x)
			cfpath = os.path.join(clpath, 'pg_config')

			# Identify a cluster by whether or not it has a pg_config link.
			if os.path.isdir(clpath) and os.path.islink(cfpath):
				c = Cluster(clpath, pg_config.dictionary(cfpath))
				clusters.append(c)
				c.version_type, vstr = c.settings['version'].split()
				c.version_info = pg_version.one.parse(vstr)

		self.clusters = clusters

	def add(self, pg_config_path, **kw):
		# Find the highest cluster number. When a new cluster is created,
		# add one to the count to get the unique name.
		high = 0
		conf = pg_config.dictionary(pg_config_path)

		# If it's not an absolute path, try to make one.
		if not os.path.isabs(pg_config_path):
			abspath = os.path.realpath(os.path.join(conf['bindir'], 'pg_config'))
			if not os.path.exists(abspath):
				raise UnresolvablePgConfig(abspath)
		else:
			abspath = pg_config_path

		for x in os.listdir(self.path):
			try:
				x = os.path.basename(x)
				if not x.startswith('cluster_'):
					continue
				num = int(x[len('cluster_'):])
			except ValueError:
				# Likely not a formal cluster
				continue
			if num > high:
				high = num
		ddirpath = os.path.join(self.path, 'cluster_%d' %(high + 1,))

		kw['superusername'] = 'tinman'
		if not kw.has_key('logfile'):
			kw['logfile'] = sys.stderr
		clust = Cluster(ddirpath, pg_config_data = conf)
		clust.init(**kw)
		clust.version_type, ver = clust.settings['version'].split()
		pgv = pg_version.one.parse(ver)
		clust.version_info = pgv
		config = {
			'log_min_messages' : 'info',
			'log_error_verbosity' : 'verbose',
			'silent_mode' : 'off',
			'port' : str(genport([
				int(x.get_parameters(('port',))['port']) for x in self.clusters
			])),
		}

		if cmp(pgv, (8, 0)) >= 0:
			config['log_destination'] = "'stderr'"
			if cmp(pgv, (8, 3)):
				config['redirect_stderr'] = 'off'
		clust.settings.update(config)

		# init logfile and its header.
		with open(os.path.join(clust.data_directory, 'log'), 'w') as f:
			f.write("%s [%s]\n[%s]\n\n" %(
				clust.data_directory,
				clust.settings['version'],
				clust.settings['configure']
			))

		os.symlink(abspath, os.path.join(ddirpath, 'pg_config'))
		self.clusters.append(clust)
		return clust

tincup_path = optparse.make_option(
	'-D', '--set-directory',
	dest = 'set_path',
	help = _('Directory where database clusters are stored')
)

# Selectivity options
select_version = optparse.make_option(
	'-V', '--pg-version',
	dest = 'version',
	help = _('Cluster with version class to select'),
	action = 'append',
	type = 'str'
)
select_cluster = optparse.make_option(
	'-c', '--cluster',
	dest = 'cluster',
	help = _('Cluster number to select [implicit OR]'),
	action = 'append',
	type = 'int'
)
select_pg_config = optparse.make_option(
	'--pg-config',
	dest = 'config_path',
	help = _('Cluster with pg_config to select [implicit OR]'),
	action = 'append',
	type = 'str'
)
select_configure = optparse.make_option(
	'--configure',
	dest = 'config',
	help = _('Cluster\'s configure string has'),
	action = 'append',
	type = 'str'
)
select_all = optparse.make_option(
	'-A', '--all',
	dest = 'all',
	help = _('Select all clusters'),
	action = 'store_true',
	default = False
)
select_type = optparse.make_option(
	'-t', '--type',
	dest = 'type',
	help = _('Select the stored selection'),
	action = 'append',
	type = 'str'
)
selection_options = [
	select_version,
	select_cluster,
	select_pg_config,
	select_configure,
	select_all,
]

COMMAND_HELP = _("""%prog [options] command [args]

Commands:
  help     Print this help menu
  list     Print paths and version information to selected clusters [ls]
  environ  Print standard PostgreSQL environment variable for connectivity [env]
  string   Print standard PostgreSQL connectivity options [str]
  path     Print path to selected clusters
  create   Create a cluster using the given pg_config
  drop     Drop the selected database clusters
  recreate Recreate the selected clusters (configuration is lost)
  stop     Signal the selected clusters to shutdown
  start    Start the selected clusters
  reload   Run pg_ctl reload on the selected clusters
  command  Run the specified command with the current cluster's environment
  set      Set the specified server parameters in each selected cluster
  show     Show the specified server parameters in each selected cluster
  readlogs Read the selected cluster's log file using $PAGER
  logs     Print absolute paths to the selected cluster's log file

Examples:
  %prog create /usr/local/pgsql/bin/pg_config
  %prog list
  %prog set "search_path=public,utils" "shared_buffers=5000"
  %prog -V 8.2 start
  %prog -V 8.1 com psql -f sql""")

def isin(txt, vals):
	for x in vals:
		if x in txt:
			return True
	return False

if os.name == 'posix':
	def exit_message(c, r):
		if r < 0:
			r = -r
			msg = _("process exited with signal")
			sys.stderr.write("%s [%s]: %s %d\n" %(
				c.data_directory, c.config['version'], msg, r,
			))
else:
	def exit_message(c, r):
		pass

def tin(args):
	if os.environ.has_key('PGTINCUP'):
		DEFAULT_TINCUP = os.environ['PGTINCUP']
	elif os.environ.has_key('HOME'):
		DEFAULT_TINCUP = os.path.join(os.environ['HOME'], '.pg_tin')
	else:
		DEFAULT_TINCUP = os.path.join(os.path.curdir, 'pg_tin')

	op = optparse.OptionParser(
		COMMAND_HELP,
		version = '1.0',
	)
	op.allow_interspersed_args = False

	op.add_options(
		selection_options
	)
	op.add_option(tincup_path)
	co, ca = op.parse_args(args[1:])

	prog = args[0]
	cup = Cup(co.set_path or DEFAULT_TINCUP)

	working_set = cup.clusters
	running_set = [x for x in working_set if x.running()]

	if co.all:
		pass
	elif not co.version and not co.config_path and \
	not co.cluster and not co.config:
		if running_set:
			working_set = running_set
	else:
		if co.version:
			for v in [pg_version.split(y) for y in co.version]:
				working_set = [
					x for x in working_set
					if x.version_info.cmp_version(v)
				]
		if co.config:
			working_set = [
				x for x in working_set
				if isin(x.config['configure'], co.config)
			]
		if co.cluster:
			ex_cl = [
				x for x in cup.clusters
				if int(
					os.path.basename(x.data_directory)[len('cluster_'):]
				) in co.cluster
			]
			if co.version or co.config:
				working_set += [x for x in ex_cl if x not in working_set]
			else:
				working_set = ex_cl
			del ex_cl
		if co.config_path:
			ex_cl = [
				x for x in cup.clusters
				if os.readlink(os.path.join(x.data_directory, 'pg_config')) \
				in co.config_path
			]
			if co.version or co.config or co.cluster:
				working += [x for x in ex_cl if x not in working_set]
			else:
				working_set = ex_cl
			del ex_cl

	while ca:
		com = ca.pop(0)
		if com in ('env', 'environ'):
			# Get environment settings to connect to a cluster
			if len(working_set) > 1:
				sys.stderr.write("Multiple clusters selected, using first.\n")
			x = working_set[0]
			sys.stdout.write('PGDATA=%s\n' %(x.data_directory))
			if 'PGUSER' not in os.environ:
				sys.stdout.write('PGUSER=tinman\n')
			sys.stdout.write('PGHOST=localhost\nPGPORT=%s\n' %(
				x.get_parameters(('port'))['port'],
			))
		elif com in ('str', 'string'):
			# Get standard PostgreSQL arguments to connect to a cluster
			if len(working_set) > 1:
				sys.stderr.write(
					_("Multiple clusters selected, using first.") + os.linesep
				)
			x = working_set[0]
			sys.stdout.write('%s-h localhost -p %s\n' %(
				('PGUSER' not in os.environ and '-U tinman ' or ''),
				x.get_parameters(('port'))['port']
			))
		elif com == 'create':
			cop = optparse.OptionParser()
			cop.add_option('-E', '--encoding')
			cop_opt, cop_arg = cop.parse_args(ca)
			kw = {}
			if cop_opt.encoding is not None:
				kw['encoding'] = cop_opt.encoding
			for x in cop_arg:
				cup.add(x, **kw)
			ca = ()
		elif com == 'drop':
			for x in working_set:
				if x.running():
					x.kill()
				x.drop()
		elif com == 'recreate':
			for x in working_set:
				pg_config_path = os.path.join(x.config['bindir'], 'pg_config')
				if x.running():
					x.stop(mode = 'immediate')
				x.drop()
				cup.clusters.remove(x)
				cup.add(pg_config_path)
		elif com in ('ls', 'list'):
			# List selected clusters
			for x in working_set:
				sys.stdout.write("%s [%s]\n" %(
					x.data_directory,
					x.config['version']
				))
		elif com == 'path':
			# Paths to selected clusters
			for x in working_set:
				sys.stdout.write("%s\n" %(x.data_directory,))
		elif com == 'name':
			# Name the selected cluster set
			pass
		elif com in ('command', 'com', 'run'):
			# Execute a command relative to the cluster selection
			path = os.environ['PATH']
			subp = ca
			ca = ()
			if not subp:
				sys.stderr.write(_("ERROR: no command specified for execution") + os.linesep)
				sys.exit(1)
			for x in working_set:
				os.environ['PGDATA'] = x.data_directory
				if 'PGUSER' not in os.environ:
					os.environ['PGUSER'] = 'tinman'
				os.environ['PGHOST'] = 'localhost'
				os.environ['PGPORT'] = x.settings['port']
				os.environ['PATH'] = x.config['bindir'] + os.path.pathsep + path
				sys.stderr.write(
					"%s[%s: %s]%s" %(
						os.linesep, 
						x.config['version'], x.data_directory,
						os.linesep,
					)
				)
				wasnt_running = not x.running()
				try:
					try:
						if wasnt_running:
							f = open(os.path.join(x.data_directory, 'log'), 'w')
							f.seek(0, 2)
							try:
								x.start(logfile = f.fileno())
							finally:
								f.close()
						try:
							oldsig = None
							p = subprocess.Popen(subp)
							oldsig = signal.signal(
								signal.SIGINT,
								lambda sn, isf: None
							)
							while True:
								try:
									exit_message(x, p.wait())
									break
								except OSError:
									pass
						finally:
							if oldsig is not None:
								signal.signal(signal.SIGINT, oldsig)
					finally:
						if wasnt_running:
							x.stop(mode = 'fast')
				except:
					sys.stderr.write("\n[%s %s]\n" %(
						x.data_directory,
						x.config['version']
					))
					traceback.print_exc(file = sys.stderr)
					continue
		elif com in ('start', 'stop', 'reload', 'restart'):
			for x in working_set:
				try:
					if com == 'start':
						os.environ['PGUSER'] = 'tinman'
						f = open(os.path.join(x.data_directory, 'log'), 'a')
						try:
							x.start(logfile = f.fileno())
						finally:
							f.close()
					elif com == 'stop':
						x.stop()
					elif com == 'reload':
						x.reload()
					elif com == 'restart':
						os.environ['PGUSER'] = 'tinman'
						f = open(os.path.join(x.data_directory, 'log'), 'a')
						f.seek(0, 2)
						try:
							x.restart(logfile = f.fileno())
						finally:
							f.close()
				except:
					sys.stderr.write("%s[%s %s]%s%s" %(
						os.linesep,
						x.data_directory,
						x.config['version'],
						os.linesep, os.linesep
					))
					traceback.print_exc(file = sys.stderr)
					sys.stderr.write("\n\rLog tail:\n\r")
					f = open(os.path.join(x.data_directory, 'log'), 'r')
					f.seek(-1024, 2)
					for x in f.read(1024).split(os.linesep)[1:]:
						sys.stderr.write(x + os.linesep)
					continue
		elif com in ('set',):
			for x in working_set:
				pd = {}
				for y in ca:
					k, v = y.split('=')
					pd[k] = v
				x.set_parameters(pd)
				if x in running_set:
					x.reload()
			ca = ()
		elif com in ('show',):
			for x in working_set:
				if not ca or ca[0] == 'all':
					p = x.get_parameters(selector = lambda x: True)
				else:
					p = x.get_parameters(ca)
				sys.stdout.write('[%s]\n' %(x.data_directory,))
				for k in p:
					sys.stdout.write('%s=%s\n' %(k, p[k]))
			ca = ()
		elif com in (_('readlogs'),):
			pager = os.environ.get('PAGER', 'less')
			if pager is None:
				sys.stderr.write(
					_('No PAGER environment variable, skipping readlog command.') + \
					os.linesep
				)
				continue
			for x in working_set:
				p = subprocess.Popen((pager, os.path.join(x.data_directory, 'log')))
				exit_message(x, p.wait())
		elif com == 'python':
			from code import interact
			interact(local = {
				'tincup' : cup,
				'program_name' : prog,
				'options' : co,
				'args' : ca
			})
		elif com in ('help', '?'):
			op.print_help()
		else:
			sys.stderr.write(
				_('unknown command %r, see %s for command help%s') %(
					com, 'help', os.linesep
				)
			)
			sys.exit(1)

if __name__ == '__main__':
	tin(sys.argv)
