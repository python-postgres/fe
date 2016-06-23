#!/usr/bin/env python
##
# setup.py - .release.distutils
##
import sys
import os

if sys.version_info[:2] < (3,3):
	sys.stderr.write(
		"ERROR: py-postgresql is for Python 3.3 and greater." + os.linesep
	)
	sys.stderr.write(
		"HINT: setup.py was ran using Python " + \
		'.'.join([str(x) for x in sys.version_info[:3]]) +
		': ' + sys.executable + os.linesep
	)
	sys.exit(1)

# distutils data is kept in `postgresql.release.distutils`
sys.path.insert(0, '')

sys.dont_write_bytecode = True
import postgresql.release.distutils as dist
defaults = dist.standard_setup_keywords()
sys.dont_write_bytecode = False

if __name__ == '__main__':
	from distutils.core import setup
	setup(**defaults)
