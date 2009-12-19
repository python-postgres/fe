##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import unittest
import warnings

from ..installation import default

from .test_exceptions import *
from .test_bytea_codec import *
from .test_iri import *
from .test_protocol import *
from .test_configfile import *
from .test_pgpassfile import *
from .test_python import *

from .test_installation import *
from .test_cluster import *

# These two require custom cluster configurations.
from .test_connect import *
# No SSL? cluster initialization will fail.
if default().ssl:
	from .test_ssl_connect import *
else:
	warnings.warn("installation doesn't support SSL")

from .test_driver import *
from .test_lib import *
from .test_dbapi20 import *

if __name__ == '__main__':
	unittest.main()
