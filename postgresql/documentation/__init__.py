import os.path
# oh noes, not egg safe
filename = os.path.join(os.path.dirname(__file__), 'index.txt')
__doc__ = open(filename).read()
__docformat__ = 'reStructured Text'
