##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Split, parse, serialize, and structure PQ IRIs.

PQ IRIs take the form::

	pq://user:pass@host:port/database?setting=value#public,othernamespace

Liking to ``https``, the sslmode can be set to "require" by::

	pqs://user:pass@host:port/database?setting=value#public,othernamespace

IPv6 is supported via the standard representation::

	pq://[::1]:5432/database
"""
from .resolved import riparse as ri

def structure(t):
	'Create a dictionary of connection parameters from a six-tuple'
	d = {}
	if t[1] is not None:
		uphp = ri.split_netloc(t[1])
		if uphp[0]:
			d['user'] = uphp[0]
		if uphp[1]:
			d['password'] = uphp[1]
		else:
			if uphp[2]:
				d['host'] = uphp[2]
			if uphp[3]:
				d['port'] = int(uphp[3])

	if t[2] is not None:
		d['database'] = t[2]

	if t[3] is not None:
		d['settings'] = dict([
			[ri.unescape(y) for y in x.split('=', 1)]
			for x in t[3].split('&')
		])

	# Path support
	if t[4] is not None:
		d['path'] = t[4].split(',')

	return d

def construct(x):
	'Construct a IRI tuple from a dictionary object'
	return (
		"pq",
		# netloc: user:pass@{host[:port]|process}
		ri.unsplit_netloc((
			x.get('user'),
			x.get('password'),
			x.get('host'),
			x.get('port')
		)),
		ri.escape_path_re.sub(x.get('database') or '', '/'),
		None if 'settings' not in x else (
			'&'.join([
				'%s=%s' %(k, v.replace('&','%26'))
				for k, v in x['settings'].items()
			])
		),
		None if 'path' not in x else ','.join(x['path'])
	)

def parse(s):
	'Parse a Postgres IRI into a dictionary object'
	return structure(ri.split(s))

def serialize(x):
	'Return a Postgres IRI from a dictionary object.'
	return ri.unsplit(construct(x))

if __name__ == '__main__':
	import sys
	for x in sys.argv[1:]:
		print("{src} -> {parsed!r} -> {serial}".format(
			src = x,
			parsed = parse(x),
			serial = serialize(parse(x))
		))
