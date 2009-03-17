#!/usr/bin/env python
"""
Entry point for pg_withcluster.
"""
import tempfile
import subprocess as sp
import getpass
from optparse import OptionParser

from ..cluster import Cluster
from ..installation import Installation
from .. import __version__

def command(argv = sys.argv):
	op = OptionParser(
		"%prog [-p] [--version] {[setting=value], ...} [pg_config] command",
		version = __version__
	)
	op.allow_interspersed_args = False
	op.add_option(
		'-l',
		dest = 'logfile',
		help = 'logfile for the PostgreSQL daemon',
		default = '/dev/stderr',
	)
	op.add_option(
		'-p',
		dest = 'look_in_path',
		help = 'Look for "pg_config" in the PATH environment',
		action = 'store_true',
		default = False
	)
	op.add_option(
		'-N',
		dest = 'max_connections',
		help = 'Number',
		type = 'int',
		default = 4,
	)
	co, ca = op.parse_args(args[1:])
	if not ca:
		sys.stderr.write("no command given to run in cluster context")
		sys.exit(1)

	max_con = co.max_connections
	if max_con <= 0:
		max_con = 4
	i = 0
	for x in ca:
		if '=' not in x:
			break
		i += 1
	settings = ca[0:i]
	command = ca[i:]
	d = tempfile.mkdtemp()
	try:
		c = Cluster(d, inn)
		c.init(
			user = getpass.getuser(),
		)
		c.settings.update([
			x.split('=', 1) for x in settings
		])
		c.settings.update({
			'port' : '14327',
			'listen_addresses' : 'localhost',
			'max_connections' : str(co.max_connections),
			'shared_buffers' : str((co.max_connections * 2) + 50),
		})
		c.start(logfile = sys.stderr)
		c.wait_until_started()
		with c.connect() as con:
			pass
		try:
			p = sp.Popen(
				command,
				env = {
					'PGUSER' : getpass.getuser(),
					'PGHOST' : 'localhost',
					'PGPORT' : c.settings['port'],
					'PGDATABASE' : getpass.getuser(),
					'PGINSTALLATION' : c.installation.pg_config_path,
					'PGDATA' : d,
					'PG_WITHCLUSTER' : '1',
				},
			)
			while True:
				try:
					return p.wait()
				except OSError as e:
					if e.errno != errno.ESRCH:
						raise
					# sigh.
		finally:
			c.stop()
	finally:
		c.drop()

if __name__ == '__main__':
	sys.exit(command(argv=sys.argv))
