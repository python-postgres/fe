# -*- encoding: utf-8 -*-
##
# copyright 2008, pg/python project.
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
import riparse as ri

def structure(t):
	'Create a dictionary of connection parameters from a six-tuple'
	d = {}

	if t[0] == 'pqs':
		d['sslmode'] = 'require'

	uphp = split_netloc(t[1])
	if uphp[0]:
		d['user'] = uphp[0]
	if uphp[1]:
		d['password'] = uphp[1]
	if uphp[2].startswith('{'):
		d['process'] = [
			unescape(x) for x in uphp[2].strip('{}').split(' ')
		]
	else:
		if uphp[2]:
			d['host'] = uphp[2]
		if uphp[3]:
			d['port'] = int(uphp[3])

	if t[2]:
		d['database'] = t[2]

	if t[3]:
		d['settings'] = dict([
			[unescape(y) for y in x.split('=', 1)]
			for x in t[3].split('&')
		])

	# Path support
	if t[4]:
		d['path'] = split_path(t[3])

	return d

def construct(x):
	'Construct a IRI tuple from a dictionary object'
	return (
		bool(x.get('ssl')) is False and 'pq' or 'pqs',
		# netloc: user:pass@{host[:port]|process}
		unsplit_netloc((
			x.get('user', ''),
			x.get('password', ''),
			x.get('process', None) is not None and '{%s}' %(
				' '.join([escape(y) for y in x['process']])
			) or x.get('host', ''),
			x.get('port', '')
		)),
		escape(x.get('database', '') or '', '/'),
		# Path
		x.get('settings', None) is not None and '&'.join([
			'%s=%s' %(k, v.replace('&','%26'))
			for k, v in x['settings'].iteritems()
		]),
		unsplit_path(x.get('path', [])),
	)

def parse(s):
	'Parse a Postgres IRI into a dictionary object'
	return structure(split(s))

def serialize(x):
	'Return a Postgres IRI from a dictionary object.'
	return unsplit(construct(x))
