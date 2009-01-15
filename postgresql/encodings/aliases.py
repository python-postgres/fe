##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Aliases for Python encodings that Postgres uses.
"""

# dictionary of Postgres encoding names to Python encoding names
postgres_to_python = {
	'unicode' : 'utf_8',
	'sql_ascii' : 'ascii',
	'euc_jp' : 'eucjp',
	'euc_cn' : 'euccn',
	'euc_kr' : 'euckr',
#	'euc_tw' : None, # N/A
#	'mule_internal' : None, # N/A
	'win1256' : 'windows_1256',
	'tcvn' : 'windows_1258',
	'win874' : 'cp874',
	'koi8r' : 'koi8_r',
	'win1251' : 'windows_1251',
	'alt' : 'cp866',
	'win1250' : 'windows_1250',
}
