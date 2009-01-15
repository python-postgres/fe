##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import sys
import os
import unittest

from postgresql.test.test_ri import *
from postgresql.test.test_environ import *
from postgresql.test.test_options import *
from postgresql.test.test_strings import *
from postgresql.test.test_types import *
from postgresql.test.test_bytea_codec import *

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
