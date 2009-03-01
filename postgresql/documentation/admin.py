##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
r"""
Administration
==============

This chapter covers the administration of py-postgresql. This includes
installation and other aspects of working with py-postgresql such as
environment variables and configuration files.


Installation
------------

py-postgresql uses Python's standard distutils package to manage the
build and installation process of the package. The normal entry point for
this is the setup.py script contained in the root project directory.

Normally, installation is as simple as::

	$ ./setup.py install

However, if you need to install for use with a particular version of python::

	$ /usr/opt/bin/python3.0 ./setup.py install

Under most POSIX systems, the above should work without problem.

Extension Modules under Windows
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, a Python installation on Windows cannot build extension modules.
py-postgresql provides optimizations for various key points, but can be
installed and used without them. When an installation is performed on 'win32'
systems, extension modules are *not* built by default.

In order to enable the compilation of extensions, set the environment variable
``PY_BUILD_EXTENSIONS`` to a non-empty string before executing the ``setup.py``
script::

	C:\-> setenv PY_BUILD_EXTENSIONS 1
	C:\-> python3 setup.py install

Or if mingw32 installed::

	C:\-> setenv PY_BUILD_EXTENSIONS 1
	C:\-> python3 setup.py build_ext --compiler=mingw32
	C:\-> python3 setup.py install


Environment
-----------

py-postgresql is not included with the PostgreSQL distribution, so it's
impossible for certain defaults to be filled in in a way that's consistent with
bundled applications.

 ``PGINSTALLATION``
  The default installation to use and reference when collecting client
  parameters or managing clusters.
"""

__docformat__ = 'reStructuredText'
if __name__ == '__main__':
	import sys
	if (sys.argv + [None])[1] == 'dump':
		sys.stdout.write(__doc__)
	else:
		try:
			help(__package__ + '.admin')
		except NameError:
			help(__name__)
