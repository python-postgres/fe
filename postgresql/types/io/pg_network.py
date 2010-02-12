from .. import INETOID, CIDROID, MACADDROID
from . import lib

#INETOID : (lib.net_pack, lib.net_unpack),
#CIDROID : (lib.net_pack, lib.net_unpack),

oid_to_io = {
	MACADDROID : (None, None),
	CIDROID : (None, None),
	INETOID : (None, None),
}
