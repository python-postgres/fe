##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
'pg_config Python interface; provides member based access to pg_config data'
import subprocess as sp
import io

def call(exe, *args):
	'helper function for the instance class'
	pa = [
		'--' + x.strip() for x in args if x is not None
	]
	pa.insert(0, exe)
	p = sp.Popen(pa,
		close_fds = True,
		stdout = sp.PIPE,
		stderr = sp.PIPE,
		stdin = sp.PIPE,
		shell = False
	)
	p.stdin.close()
	rv = p.wait()
	if rv != 0:
		return None
	return io.TextIOWrapper(p.stdout).read()

def dictionary(pg_config_path):
	"""
	Create a dictionary of the information available in the given pg_config_path.
	This provides a one-shot solution to fetching information from the pg_config
	binary.
	"""
	default_output = call(pg_config_path)
	if default_output is not None:
		d = {}
		for x in call(pg_config_path).splitlines():
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
	for l in call(pg_config_path, 'help').splitlines():
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

	d=dict(zip(opt, call(pg_config_path, *opt).splitlines()))
	d['version'] = call(pg_config_path, 'version').strip()
	return d
