##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
"""
itertools extensions
"""
import collections
from itertools import cycle

def interlace(*iters) -> collections.Iterable:
	"""
	interlace(i1, i2, ..., in) -> (
		i1-0, i2-0, ..., in-0,
		i1-1, i2-1, ..., in-1,
		.
		.
		.
		i1-n, i2-n, ..., in-n,
	)
	"""
	iters = [iter(x) for x in iters]
	for i in cycle(range(len(iters))):
		yield next(iters[i])
