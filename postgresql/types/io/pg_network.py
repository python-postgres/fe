from .. import INETOID, CIDROID, MACADDROID
from . import lib
try:
	import ipaddress
	oid_to_type = {
		MACADDROID : str,
		INETOID: ipaddress._IPAddressBase,
		CIDROID: ipaddress._BaseNetwork,
		}
	def inet_pack(ob, pack = lib.net_pack, Constructor = ipaddress.ip_address):
		a = Constructor(ob)
		return pack((a.version, None, a.packed))

	def cidr_pack(ob, pack = lib.net_pack, Constructor = ipaddress.ip_network):
		a = Constructor(ob)
		return pack((a.version, a.prefixlen, a.network_address.packed))

	def inet_unpack(data, unpack = lib.net_unpack, Constructor = ipaddress.ip_address):
		version, mask, data = unpack(data)
		return Constructor(data)

	def cidr_unpack(data, unpack = lib.net_unpack, Constructor = ipaddress.ip_network):
		version, mask, data = unpack(data)
		return Constructor(data).supernet(new_prefix=mask)

except ImportError:
	import ipaddr as ipaddress
	oid_to_type = {
		MACADDROID : str,
		INETOID: ipaddress._IPAddrBase,
		CIDROID: ipaddress._BaseNet,
		}
	def inet_pack(ob, pack = lib.net_pack, Constructor = ipaddress.IPAddress):
		a = Constructor(ob)
		return pack((a.version, None, a.packed))

	def cidr_pack(ob, pack = lib.net_pack, Constructor = ipaddress.IPNetwork):
		a = Constructor(ob)
		return pack((a.version, a.prefixlen, a.network.packed))

	def inet_unpack(data, unpack = lib.net_unpack, Constructor = ipaddress.IPAddress):
		version, mask, data = unpack(data)
		return Constructor(data)

	def cidr_unpack(data, unpack = lib.net_unpack, Constructor = ipaddress.IPNetwork):
		version, mask, data = unpack(data)
		return Constructor(data).supernet(new_prefix=mask)



oid_to_io = {
	MACADDROID : (lib.macaddr_pack, lib.macaddr_unpack, str),
	CIDROID : (cidr_pack, cidr_unpack, str),
	INETOID : (inet_pack, inet_unpack, str),
}
