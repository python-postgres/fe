##
# copyright 2008, pg/python project.
# http://python.projects.postgresql.org
##
"""
Parse and construct DSN strings.
"""
import postgresql.strings as pg_str
import re

def split(dsn):
	for x in dsn.split():
		yield x.split('=', 1)

def parse(s):
	'Parse a DSN into a dictionary object'
	return dict(split(s))
