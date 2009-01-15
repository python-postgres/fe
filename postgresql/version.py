##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
PostgreSQL version parsing.

>>> postgresql.version.split('8.0.1')
(8, 0, 1, None, None)

>>> postgresql.version.compare(
...  postgresql.version.split('8.0.1'),
...  postgresql.version.split('8.0.1'),
... )
0
"""

def split(vstr):
	"""
	Split a PostgreSQL version string into a tuple
	(major,minor,patch,state_class,state_level)
	"""
	v = vstr.strip().split('.', 3)

	# Get rid of the numbers around the state_class (beta,a,dev,alpha)
	state_class = v[-1].strip('0123456789')
	if state_class:
		last_version_num, state_level = v[-1].split(state_class)
		if not state_level:
			state_level = None
		else:
			state_level = int(state_level)
	else:
		last_version_num = v[-1]
		state_level = None
		state_class = None

	if last_version_num:
		last_version_num = int(last_version_num)
	else:
		last_version_num = None
	
	if len(v) == 3:
		major = int(v[0])
		if v[1]:
			minor = int(v[1])
		else:
			minor = None
		patch = last_version_num
	elif len(v) == 2:
		major = int(v[0])
		minor = last_version_num
		patch = None
	else:
		major = last_version_num
		minor = None
		patch = None

	return (
		major,
		minor,
		patch,
		state_class,
		state_level
	)

def unsplit(vtup):
	'join a version tuple back into a version string'
	return '%s%s%s%s%s' %(
		vtup[0],
		vtup[1] is not None and '.' + str(vtup[1]) or '',
		vtup[2] is not None and '.' + str(vtup[2]) or '',
		vtup[3] is not None and str(vtup[3]) or '',
		vtup[4] is not None and str(vtup[4]) or ''
	)

default_state_class_priority = [
	'dev',
	'a',
	'alpha',
	'b',
	'beta',
	'rc',
	None,
]

def compare(
	v1, v2,
	state_class_priority = default_state_class_priority,
	cmp = cmp
):
	"""
	Compare the given versions using the given `cmp` function after translating
	the state class of each version into a numeric value derived by the class's
	position in the given `state_class_priority`.

	`cmp` and `state_class_priority` have default values.
	"""
	v1l = list(v1)
	v2l = list(v2)
	try:
		v1l[-2] = state_class_priority.index(v1[-2])
	except ValueError:
		raise ValueError("first argument has unknown state class %r" %(v1[-2],))
	try:
		v2l[-2] = state_class_priority.index(v2[-2])
	except ValueError:
		raise ValueError("second argument has unknown state class %r" %(v2[-2],))
	return cmp(v1l, v2l)


def python(self):
	return repr(self)

def xml(self):
	return '<version type="one">\n' + \
		' <major>' + str(self[0]) + '</major>\n' + \
		' <minor>' + str(self[1]) + '</minor>\n' + \
		' <patch>' + str(self[2]) + '</patch>\n' + \
		' <state>' + str(self[3]) + '</state>\n' + \
		' <level>' + str(self[4]) + '</level>\n' + \
		'</version>'

def sh(self):
	return """PG_VERSION_MAJOR=%s
PG_VERSION_MINOR=%s
PG_VERSION_PATCH=%s
PG_VERSION_STATE=%s
PG_VERSION_LEVEL=%s""" %(
		str(self[0]),
		str(self[1]),
		str(self[2]),
		str(self[3]),
		str(self[4]),
	)

if __name__ == '__main__':
	import sys
	from optparse import OptionParser
	op = OptionParser()
	op.add_option('-f', '--format',
		type='choice',
		dest='format',
		help='format of output information',
		choices=('sh', 'xml', 'python'),
		default='sh',
	)
	op.add_option('-t', '--type',
		type='choice',
		dest='type',
		help='type of version string to parse',
		choices=('auto', 'one',),
		default='auto',
	)
	op.set_usage(op.get_usage().strip() + ' "version to parse"')
	co, ca = op.parse_args()
	if len(ca) != 1:
		op.error('requires exactly one argument, the version')
	if co.type != 'auto':
		v = getattr(sys.modules[__name__], co.type).parse(ca[0])
	else:
		v = split(ca[0])
	sys.stdout.write(getattr(v, co.format)())
	sys.stdout.write('\n')
