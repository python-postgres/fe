##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
import unittest
import postgresql.clientoptparse as client_options

class test_options(unittest.TestCase):
	def testOptions(self):
		import optparse
		optparser = optparse.OptionParser(
			"%prog", version = "0",
			add_help_option = False
		)
		optparser.add_options(client_options.default)
		args = {
			'-d' : 'my_dbname',
			'-p' : '5432',
			'-U' : 'my_username',
			'--server-options' : 'my_options',
		}
		l = []
		for x, y in args.iteritems():
			l.append(x)
			l.append(y)
		co, ca = optparser.parse_args(l)
		for x in args:
			for y in client_options.default:
				if x.startswith('--'):
					if x in y._long_opts:
						break
				else:
					if x in y._short_opts:
						break
			self.failUnless(
				args[x] == str(getattr(co, y.dest)),
				"parsed argument %r did not map to value %r, rather %r" %(
					x, args[x], getattr(co, y.dest)
				)
			)

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
