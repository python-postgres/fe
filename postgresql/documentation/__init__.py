##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
Contents:

 Index
  `postgresql.documentation.index`

 Driver Basics
  `postgresql.documentation.driver_basics`

 Gotchas
  `postgresql.documentation.gotchas`
"""
__docformat__ = 'reStructured Text'

# -m rejects this, so make the .index module the, well, index.
if __name__ == '__main__':
	import sys
	if (sys.argv + [None])[1] == 'dump':
		sys.stdout.write(__doc__)
	else:
		try:
			help(__package__)
		except NameError:
			help(__name__)
