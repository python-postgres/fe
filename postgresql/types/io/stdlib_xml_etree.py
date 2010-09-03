##
# types.io.stdlib_xml_etree
##
try:
	import xml.etree.cElementTree as etree
except ImportError:
	import xml.etree.ElementTree as etree
from .. import XMLOID
from ...python.functools import Composition as compose

oid_to_type = {
	XMLOID: etree.ElementTree,
}

def xml_unpack(xmldata, XML = etree.XML):
	try:
		return XML(xmldata)
	except Exception:
		# try it again, but return the sequence of children.
		return tuple(XML('<x>' + xmldata + '</x>'))

if not hasattr(etree, 'tostringlist'):
	# Python 3.1 support.
	def xml_pack(xml, tostr = etree.tostring, et = etree.ElementTree,
		str = str, isinstance = isinstance, tuple = tuple
	):
		if isinstance(xml, str):
			# If it's a string, encode and return.
			return xml
		elif isinstance(xml, tuple):
			# If it's a tuple, encode and return the joined items.
			# We do not accept lists here--emphasizing lists being used for ARRAY
			# bounds.
			return ''.join((x if isinstance(x, str) else tostr(x) for x in xml))
		return tostr(xml)

	def xml_io_factory(typoid, typio, c = compose):
		return (
			c((xml_pack, typio.encode)),
			c((typio.decode, xml_unpack)),
			etree.ElementTree,
		)
else:
	# New etree tostring API.
	def xml_pack(xml, encoding,
		tostr = etree.tostringlist, et = etree.ElementTree,
		str = str, isinstance = isinstance, tuple = tuple,
	):
		if isinstance(xml, str):
			# If it's a string, encode and return.
			return xml.encode(encoding)
		elif isinstance(xml, tuple):
			# If it's a tuple, encode and return the joined items.
			# We do not accept lists here--emphasizing lists being used for ARRAY
			# bounds.
			return b''.join((
				x.encode(encoding) if isinstance(x, str) else
				b''.join(tostr(x, encoding = encoding)[1:]) for x in xml
			))
		return b''.join(tostr(xml, encoding = encoding)[1:])

	def xml_io_factory(typoid, typio, c = compose):
		def local_xml_pack(x, typio = typio):
			return xml_pack(x, typio.encoding)
		return (
			local_xml_pack,
			c((typio.decode, xml_unpack)),
			etree.ElementTree,
		)

oid_to_io = {
	XMLOID : xml_io_factory
}
