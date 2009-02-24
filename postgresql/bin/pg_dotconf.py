#!/usr/bin/env python
import sys
from optparse import OptionParser
from postgresql.configfile import pg_cf

__all__ = ['pg_dotconf']

def command(args):
	"""
	pg_dotconf script entry point.
	"""
	op = OptionParser(
		"%prog [--stdout] [-f settings] config_file_src [param=val]+",
		version = '0'
	)
	op.add_option(
		'-f', '--file',
		dest = 'settings',
		help = 'The file of settings to push; parameter arguments may override',
		default = [],
		action = 'append',
	)
	op.add_option(
		'--stdout',
		dest = 'stdout',
		help = 'Redirect the changed file to standard output',
		action = 'store_true',
		default = False
	)
	co, ca = op.parse_args(args[1:])
	if not ca:
		return 0

	settings = {}
	for sfp in co.settings:
		with open(sfp) as sf:
			for line in sf:
				pl = pg_cf.parse_line(line)
				if pl is not None:
					if comment not in line[pl[0].start]:
						settings[line[pl[0]]] = unquote(line[pl[1]])

	for p in ca[1:]:
		if p.startswith('#'):
			k = p[1:]
			v = None
		elif '=' not in p:
			sys.stderr.write(
				"ERROR: setting parameter, %r, does not have '=' " \
				"to separate setting name from setting value"
			)
			return 1
		else:
			k, v = p.split('=', 1)
		settings[k] = v

	fp = ca[0]
	with open(fp, 'r') as fr:
		lines = pg_cf.alter_config(settings, fr)

	if co.stdout or fp == '/dev/stdin':
		for l in lines:
			sys.stdout.write(l)
	else:
		with open(fp, 'w') as fw:
			for l in lines:
				fw.write(l)
	return 0

if __name__ == '__main__':
	sys.exit(command(sys.argv))
