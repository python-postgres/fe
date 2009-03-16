#!/usr/bin/env python
##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import sys
import os

if sys.version_info[:2] < (3,0):
	sys.stderr.write(
		"ERROR: py-postgresql is for Python 3.0 and greater." + os.linesep
	)
	sys.stderr.write(
		"HINT: setup.py was ran using Python " + \
		'.'.join([str(x) for x in sys.version_info[:3]]) + 
		': ' + sys.executable + os.linesep
	)
	sys.exit(1)

# distutils data is kept in `postgresql.release.distutils`
sys.path.insert(0, '')

# Only build extension modules on win32 if PY_BUILD_EXTENSIONS is enabled.
# People who get failures are more likely to just give up on the package
# without reading the documentation. :(
build_extensions = (
	os.environ.get('PY_BUILD_EXTENSIONS') \
	or not sys.platform in ('win32',)
)
if build_extensions == '0':
	build_extensions = False

import postgresql.release.distutils as pg_dist
defaults = pg_dist.standard_setup_keywords(
	build_extensions = build_extensions
)
if __name__ == '__main__':
	from distutils.core import setup
	setup(**defaults)
