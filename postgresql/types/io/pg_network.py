from .. import INETOID, CIDROID, MACADDROID
from . import lib
import ipaddress

oid_to_type = {
	MACADDROID : str,
	INETOID: ipaddress._IPAddressBase,
	CIDROID: ipaddress._BaseNetwork,
}

def inet_pack(ob, pack = lib.net_pack, Constructor = ipaddress.ip_interface):
	a = Constructor(ob)
	return pack((a.version, a.network.prefixlen, a.packed))

def cidr_pack(ob, pack = lib.net_pack, Constructor = ipaddress.ip_network):
	a = Constructor(ob)
	return pack((a.version, a.prefixlen, a.network_address.packed))

def inet_unpack(data, unpack = lib.net_unpack, Constructor = ipaddress.ip_interface):
	version, mask, data = unpack(data)
	if (version == 4 and mask == 32) or (version == 6 and mask == 128):
		return ipaddress.ip_address(data)
	else:
		return Constructor("{addr}/{mask}".format(addr=ipaddress.ip_address(data), mask=mask))

def cidr_unpack(data, unpack = lib.net_unpack, Constructor = ipaddress.ip_network):
	version, mask, data = unpack(data)
	return Constructor(data).supernet(new_prefix=mask)

oid_to_io = {
	MACADDROID : (lib.macaddr_pack, lib.macaddr_unpack, str),
	CIDROID : (cidr_pack, cidr_unpack, str),
	INETOID : (inet_pack, inet_unpack, str),
}
