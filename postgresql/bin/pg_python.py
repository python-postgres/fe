##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Python command with a postgresql.driver.pq3 connection.
"""
import os
import sys
import re
import code
import optparse
import contextlib
from .. import clientparameters
from ..resolved import pythoncommand as pycmd

from ..driver import default as pg_driver

pq_trace = optparse.make_option(
	'--pq-trace',
	dest = 'pq_trace',
	help = 'trace PQ protocol transmissions',
	default = None,
)
default_options = [
	clientparameters.option_in_xact,
	pq_trace,
] + pycmd.default_optparse_options

param_pattern = re.compile(
	r'^\s*#\s+-\*-\s+postgresql\.([^:]+):\s+([^\s]*)\s+-\*-\s*$',
	re.M
)
def extract_parameters(src):
	'extract hard parameters out of the "-*- postgresql.*: -*-" magic lines'
	return [
		x for x in re.findall(param_pattern, src)
	]

def command(args = sys.argv):
	p = clientparameters.DefaultParser(
		"%prog [connection options] [script] ...",
		version = '1.0',
		option_list = default_options
	)
	p.disable_interspersed_args()
	co, ca = p.parse_args(args[1:])
	in_xact = co.in_xact

	cond = clientparameters.standard(co = co, prompt_title = "pg_python")
	connector = pg_driver.create(**cond)
	connection = connector.create()

	pythonexec = pycmd.Execution(ca,
		context = getattr(co, 'python_context', None),
		loader = getattr(co, 'python_main', None),
	)
	# Some points of configuration need to be demanded by a script.
	src = pythonexec.get_main_source()
	if src is not None:
		hard_params = dict(extract_parameters(src))
		if hard_params:
			iso = hard_params.get('isolation')
			if iso is not None:
				if iso == 'none':
					in_xact = False
				else:
					in_xact = True
					connection.xact(isolation = iso)

	builtin_overload = {
	# New built-ins
		'connector' : connector,
		'db' : connection,
		'prepare' : connection.prepare,
		'execute' : connection.execute,
		'settings' : connection.settings,
		'proc' : connection.proc,
		'xact' : connection.xact,
	}
	if not isinstance(__builtins__, dict):
		builtins_d = __builtins__.__dict__
	else:
		builtins_d = __builtins__
	restore = {k : builtins_d.get(k) for k in builtin_overload}

	trace_file = None
	if co.pq_trace is not None:
		trace_file = open(co.pq_trace, 'a')
	builtins_d.update(builtin_overload)
	try:
		if trace_file is not None:
			connection.tracer = trace_file.write
		context = [connection]
		if in_xact:
			context.append(connection.xact)
		with contextlib.nested(*context):
			rv = pythonexec(
				context = pycmd.postmortem(os.environ.get('PYTHON_POSTMORTEM'))
			)
	finally:
		# restore __builtins__
		builtins_d.update(restore)
		for k, v in builtin_overload.items():
			if v is None:
				del builtins_d[x]
		if trace_file is not None:
			trace_file.close()
	return rv

if __name__ == '__main__':
	sys.exit(command(sys.argv))
##
# vim: ts=3:sw=3:noet:
