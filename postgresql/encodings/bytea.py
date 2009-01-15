##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
'PostgreSQL bytea encoding registration'
import codecs

try:
	from postgresql.encodings.cbytea import *
except ImportError:
	from postgresql.encodings.pbytea import *

bytea_codec = (Codec.encode, Codec.decode, StreamReader, StreamWriter)
codecs.register(lambda x: x == 'bytea' and bytea_codec or None)
