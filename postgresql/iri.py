##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Split, parse, serialize, and structure PQ IRIs.

PQ IRIs take the form::

	pq://user:pass@host:port/database?setting=value&setting2=value2#public,othernamespace

IPv6 is supported via the standard representation::

	pq://[::1]:5432/database

Driver Parameters:

	pq://user@host/?[driver_param]=value&[other_param]=value
"""
from .resolved import riparse as ri
import re

escape_path_re = re.compile('[%s]' %(re.escape(ri.unescaped + ','),))

def structure(d, fieldproc = ri.unescape):
	'Create a clientparams dictionary from a parsed RI'
	if d.get('scheme', 'pq') != 'pq':
		raise ValueError("not a PQ-IRI")

	cpd = {
		k : fieldproc(v) for k, v in d.items()
		if k not in ('path', 'fragment', 'query', 'host', 'scheme')
	}
	path = d.get('path')
	frag = d.get('fragment')
	query = d.get('query')
	host = d.get('host')

	if host:
		if host.startswith('[') and host.endswith(']'):
			cpd['ipv'] = 6
			cpd['host'] = host[1:-1]
		else:
			cpd['host'] = fieldproc(host)

	if path:
		if len(path) > 1:
			raise ValueError("PQ-IRIs may only have one path component")
		# Only state the database field's existence if the first path is non-empty.
		if path[0]:
			cpd['database'] = path[0]

	if frag:
		d['path'] = [
			fieldproc(x) for x in frag.split(',')
		]

	if query:
		settings = {}
		for k, v in query.items():
			if k.startswith('[') and k.endswith(']'):
				k = k[1:-1]
				if k != 'settings' and k not in cpd:
					cpd[fieldproc(k)] = fieldproc(v)
			elif k:
				settings[fieldproc(k)] = fieldproc(v)
			# else: ignore empty query keys
		if settings:
			cpd['settings'] = settings

	return cpd

def construct_path(x, re = escape_path_re):
	"""
	Join a path sequence using ',' and escaping ',' in the pieces.
	"""
	return ','.join((
		re.sub(ri.re_pct_encode, y) for y in x
	))

def construct(x):
	'Construct a RI dictionary from a clientparams dictionary'
	return (
		'pq',
		# netloc: user:pass@{host[:port]|process}
		ri.unsplit_netloc((
			x.get('user'),
			x.get('password'),
			'[' + x.get('host') + ']' if (
				str(x.get('ipv', -1)) == '6' and ':' in x.get('host', '')
			) else x.get('host'),
			x.get('port')
		)),
		None if 'database' not in x else (
			ri.escape_path_re.sub(x['database'], '/')
		),
		None if 'settings' not in x else (
			ri.construct_query(x['settings'])
		),
		None if 'path' not in x else construct_path(x['path']),
	)

def parse(s, fieldproc = ri.unescape):
	'Parse a Postgres IRI into a dictionary object'
	return structure(
		# In ri.parse, don't unescape the parsed values as our sub-structure
		# uses the escape mechanism in IRIs to specify literal separator
		# characters.
		ri.parse(s, fieldproc = str),
		fieldproc = fieldproc
	)

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
