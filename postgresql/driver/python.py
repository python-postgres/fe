# -*- encoding: utf-8 -*-
# $Id$
##
# copyright 2007, pg/python project.
# http://python.projects.postgresql.org
##
"""
Python command with a postgresql.driver.pg_api connection.
"""
import os
import optparse
import postgresql.commandoptions as pg_opt
import postgresql.clientparams as clientparams
import postgresql.python as pg_python
import postgresql.driver.pg_api as pg_api

pq_trace = optparse.make_option(
	'--pq-trace',
	dest = 'pq_trace',
	help = 'trace PQ protocol transmissions',
	default = None,
)
default_options = pg_python.default_options + [
	pq_trace,
]

def command(args, environ = os.environ):
	# Allow connection options to be collected in #!pg_python lines
	p = pg_opt.DefaultParser(
		"%prog [connection options] [script] [-- script options] [args]",
		version = '1.0',
		option_list = default_options
	)
	p.enable_interspersed_args()
	co, ca = p.parse_args(args[1:])

	cond = clientparams.create(co, environ = environ)
	connector = pg_api.connector(**cond)
	connection = connector.create()

	trace_file = None
	if co.pq_trace is not None:
		trace_file = open(co.pq_trace, 'a')
	try:
		if trace_file is not None:
			connection.tracer = trace_file.write
		return pg_python.run(
			connection, ca, co,
			in_xact = co.in_xact,
			environ = environ
		)
	finally:
		if trace_file is not None:
			trace_file.close()

def pg_python():
	import sys
	sys.exit(command(sys.argv))

if __name__ == '__main__':
	pg_python()
##
# vim: ts=3:sw=3:noet:
