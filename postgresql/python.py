##
# copyright 2008, pg/python project.
# http://python.projects.postgresql.org
##
'Provisions for making a Python command with a PostgreSQL connection'
import os
import sys
import re
import code
import python.command.runtime as cmd_rt
import python.command.optparse_options as cmd_opt
import postgresql.clientoptparse as pg_opt

default_options = [
	pg_opt.in_xact,
] + cmd_opt.default

class SavingConsole(cmd_rt.ExtendedConsole):
	def __init__(self, *args, **kw):
		cmd_rt.ExtendedConsole.__init__(self, *args, **kw)
		self.autosave = False
		self.register_backslash('\\s', self.toggle_autosave,
			"Wrap each execution code of in a new SAVEPOINT; Rollback on exception"
		)

	def toggle_autosave(self, cmd, arg):
		if self.autosave:
			self.autosave = False
			self.write("[Automatic savepointing Disabled]" + os.linesep)
		else:
			if pg.xact.closed:
				pg.xact.start()
			elif pg.xact.failed:
				pg.xact.reset()
				pg.xact.start()
			self.autosave = True
			self.write("[Automatic savepointing Enabled]" + os.linesep)

	def runcode(self, codeob):
		if self.autosave:
			return self.runsavedcode(codeob)
		else:
			return cmd_rt.ExtendedConsole.runcode(self, codeob)

	def runsavedcode(self, codeob):
		try:
			try:
				if not pg.closed:
					pg.xact.start()
			except:
				self.showtraceback()
				exec(codeob, globals(), self.locals)
			else:
				try:
					exec(codeob, globals(), self.locals)
				except SystemExit as e:
					if not pg.closed:
						if e.code == 0:
							pg.xact.commit()
						else:
							pg.xact.abort()
					raise
				except:
					if not pg.closed:
						pg.xact.abort()
					raise
				else:
					if not pg.closed:
						pg.xact.commit()
		except SystemExit:
			raise
		except:
			self.showtraceback()

param_pattern = re.compile(
	r'^\s*#\s+-\*-\s+postgresql\.([^:]+):\s+([^\s]*)\s+-\*-\s*$',
	re.M
)
def extract_parameters(src):
	'extract hard parameters out of the "-*- postgresql.*: -*-" magic lines'
	return [
		x for x in re.findall(param_pattern, src)
	]

def run(
	connection, ca, co, in_xact = False, environ = os.environ
):
	pythonexec = cmd_rt.execution(ca,
		context = getattr(co, 'python_context', None),
		loader = getattr(co, 'python_main', None),
		postmortem = co.python_postmortem,
	)
	builtin_overload = {
	# New built-ins
		'pgc' : connection,
		'pg' : connection,
	# Deprecating
		'gtc' : connection.connector,
		'gtx' : connection,
	#
		'query' : connection.query,
		'cquery' : connection.cquery,
		'statement' : connection.statement,
		'execute' : connection.execute,
		'settings' : connection.settings,
		'cursor' : connection.cursor,
		'proc' : connection.proc,
		'xact' : connection.xact,
	}
	__builtins__.update(builtin_overload)

	# Some points of configuration need to be demanded by a script.
	src = pythonexec.get_main_source()
	if src is not None:
		hard_params = dict(extract_parameters(src))
		if hard_params:
			if hard_params.get('disconnected', 'false') != 'true':
				connection.connect()
			else:
				in_xact = False

			iso = hard_params.get('isolation')
			if iso is not None:
				if iso == 'none':
					in_xact = False
				else:
					in_xact = True
					connection.xact(isolation = iso)
		else:
			connection.connect()
	else:
		connection.connect()

	try:
		if in_xact is True:
			connection.xact.start()
		try:
			rv = pythonexec(
				console = SavingConsole,
				environ = environ
			)
		except:
			if in_xact is True:
				connection.xact.abort()
			raise
		if in_xact is True:
			if rv == 0:
				connection.xact.commit()
			else:
				connection.xact.abort()
	finally:
		connection.close()
		for x in builtin_overload.keys():
			del __builtins__[x]
	return rv
##
# vim: ts=3:sw=3:noet:
