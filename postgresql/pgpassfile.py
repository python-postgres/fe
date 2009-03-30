##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
'Parse pgpass files and subsequently lookup a password.'
import os
import csv

def parse(data):
	'produce a list of [(word, (host,port,dbname,user))] from a pgpass file object'
	return [
		(x[-1], x[0:4])
		for x in csv.reader(data,
			delimiter = ':',
			escapechar = '\\',
			quotechar = '\x00',
			doublequote = False,
			skipinitialspace = False,
			lineterminator = os.linesep,
			quoting = csv.QUOTE_NONE,
		) if x and not x[0].startswith('#')
	]

def lookup_password(words, uhpd):
	"""
	lookup_password(words, (user, host, port, database)) -> password

	Where 'words' is the output from pgpass.parse()
	"""
	user, host, port, database = uhpd
	for word, (w_host, w_port, w_database, w_user) in words:
		if (w_user == '*' or w_user == user) and \
			(w_host == '*' or w_host == host) and \
			(w_port == '*' or w_port == port) and \
		(w_database == '*' or w_database == database):
			return word

def lookup_password_file(path, t):
	'like lookup_password, but takes a file path'
	with open(path) as f:
		return lookup_password(parse(f), t)

def lookup_pgpass(d, passfile):
	# If the password file exists, lookup the password
	# using the config's criteria.
	if os.path.exists(passfile):
		return lookup_password_file(passfile, (
			str(d['user']), str(d['host']), str(d['port']),
			str(d.get('database', d['user']))
		))
