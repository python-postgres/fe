import os.path
# oh noes, not egg safe
filepath = os.path.join(
	os.path.dirname(__file__),
	__name__.split('.')[-1] + '.txt'
)
__doc__ = open(filepath).read()
__docformat__ = 'reStructured Text'
