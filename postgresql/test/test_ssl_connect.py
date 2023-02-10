##
# .test.test_ssl_connect
##
import sys
import os
import unittest

from .. import exceptions as pg_exc
from .. import driver as pg_driver
from ..driver import dbapi20
from . import test_connect

default_installation = test_connect.default_installation

has_ssl = False
if default_installation is not None:
	has_ssl = default_installation.ssl

server_key = """
-----BEGIN RSA PRIVATE KEY-----
MIIJKQIBAAKCAgEAp6C6t3exwgx5QQjeoW2vtawSl9SMhsNKfwGVh97gStBCHNqZ
DuO6nn5qp3GmzkDII+B8uAJPe5znHSlqj2g13EiFENeaF3G9l1uzaWGEvuFyU2sq
x3lu/pJz6ISEhlogkrGz9inmMcLaNLzm4XbXR/9pjf3QKq7xPH0CacjSzeA9gfAm
CjKJM/DxkrWeyKvBJuVCZbDPbCHtS1MJvAcU0DL9wdfPr4+2P4rVjzBgbzUzPUXL
DT/ewAk94aJPZAWAvtNrdbXjSvIJ/CWBedLtyCpHPchRwaOdJrkZYItYRqYP7SKM
rwddTbrQ/70sCHCS9Yq7X6NO9ONNVrgLhVQm3Ua3FsGyKcU/bx+xEYQsAsCJj7Ps
WdRhImU/3bdHqPobwyKRbssa5iz1rrwdQ0eFUakv+he3nqXLUqmqOs4RrrM5OvRs
e/JCi5N50NlRXkiix2u909vCdPQFzviiVQbkpqzSmejN0PF3GYFpc0+c7HDp78J9
YEd+WMvx16LABVy9Kq5eYQbGQOmaWzH01fH+h3vxGnA4G9ArXGPL93P0+ztNhHJf
XBg5bwNzy5cca4roy6QNx87M/+n23iEHE4Bn9uulYJsXx2urUOAN9WCJTKYULTfu
IChWIRDy5ceYVcedHiuhRO90WyGsmwILAoAV0jebDosMK6Q+kIoOM2DT1n8CAwEA
AQKCAgAoDKTPtM9Jl4VY3m+ijfxPIX+HuwagJASmd5BsV/mqpjtFfYzYG9y4hWeh
/etml9+5gqcJp7OpywEE3KJTBQjpSoJQVdLBCzHK+ePRp7T5jg+skow0AHVeaUs8
IH0xRFNH+SEQDU6sUOulcgSPlb81unZTsHKN4CJO22c6Mvr6qTrI0sGj6hMRz91H
uhDnzPFnA5trhGTqZui0+G/49pAodiZeq9s5DNL0N41ympJPv5wwZX5v+fSUWSDp
ycfCE/aAoS6pfv2BKHbuQV+/5X9eNYuz3Sp7Y0XmvI6tnF1I8+AWPgzyvIW0TpAk
qePdWFgkRjMiVHhG1g/iSjKmdkaacIqfOmaaUO//r5uj8L4rSxcTtqfM4CoUwiGo
Iqj0fCMQp+G+QCyMJzm0d2Ctg5mOMxFbl3jk09Z2u/ZaUpauvJ0S8WIEkJ0BMdGN
AqOtBFD7xOo6od2+7zreBVJQAV15owwi578Jk86skp5zY400IlV51yLqM6BQa3zd
Ft4xF2up0e/n7xVWW3twY8m/4i+ie+20hap9730UENo1XGy9iIIN8bxHU9+NcHdL
AP71Zgqa2nC2Wy7sCdRkt8c7P2VuraOoWgsOFvShhNOdVWR/LH/WwQaBzG9nj2u3
0hmaDJezjdBGEJk3EhNocrMxYV1+L/MBgT6jBABx0b98K9YAEQKCAQEA03u/PPBQ
9H3ybGHC/JpfG+hLd6/Ux8Eq69i4rrduYshRFnwKtSjyfAPdtpLPhx3/N3uuxmCK
2VO+hwMKCEgk8Sb/qp0z2Dthvl2KCHUXOilFEh6B5J1nQi3OJVxalR3/yDp3ir8F
TdptY1gybWBFGdwnKSdHEiCr3+3k9OoSPeqYUuHuzBw2s8Zcet8RirJbtJVv5VyY
WNnzv/vDaEsMLENm0opmEAkFw+YW3ltolgPXyKfmgtXOKAk2s04DEwC0sHmiUvBY
4CdX6TBD7DXNEkl+bpOA5j92USGrlAiKkxeq+igQ7dPDhlmYcAahJodV3fyBPwqz
5pa6SWxQMNhRYwKCAQEAyum6pSHGHoI8twxpf/sgG3wKwUN+BQXoKustueeU66GV
P5xN+4tFmxJCRegFnfRB/IS9Oi5tety1BgYUA8z7h3pRz3ed3FUF0UXCDhgnqmp3
XWpa9MBkoA8MO+/s+k10CZz5doR9cS+l2c7XfrlwHn21juScfGEsaxhgGBYbDVlW
IehjNERjVYyl4oHG9H/baXGRLaYfaFummwNGivWI0kqn8b00Sc0uW28LAmze5/IM
2simidgDJjV8EScta8o8uF6fe/3WKvGas7/NwVW+zP+Rs/sgsqa9FHQ2FZYRzIrw
5VpnGbz69SxRkbqLdPoKNQrcGOUdDmXrNZds1BAfNQKCAQEArztnHzhE7AD8ASAU
L7g9vGMDPT3dQlLlnJxrkqF8/q7auZW4TZmLKoUNjf0hpeSOF0wNamSOSDtisH4t
LuWQbp0Q1S8CyVWSzOi2ugFDaLbPe475tBNUfvpzSHO4vrwnt6HycW2MGJE3eEyZ
JBXTy/SmIixgcD3QDHES+HiG+vTKmEqK0mdCUD25XTo+T70vzXbRS6wos96MYPRc
Wqtsf7StmyCAJyNCuqqJIl99TmgKwUGV96zu8C+KOpIWbAV2so9ml/B8w+b1qcuL
TErcDB4He9oOwTmucNVEVRmqsOy4iCTwug9wgH72l0R2/PTAinpyIWld3V/hJXtx
CrgC3wKCAQEAiTTQk3ap+9k+6tvGvtZ1WIBg2Vwk64qZ+eN60PlKFqb1P8UWaiA7
mecXzyNcIPmYYQL03VGlj+2Lrp4PjJ5f+rT4etw8b09ClsafuF4W/EHvosgW5ubt
Y9mpASJ0ULBs5U8y1DQ0ioOYlxYpWzRTHxsL2Kq3MdeXbHdYCxFvi3A8MMNtyVrw
/FkVlnsAqDWIjN1RONfa5vsKRklJuw7aTLBUrb6ti7XlQchtXl91vstKa+o/yne5
cW27DfI64Wcn9ddt6i6zUeh7Hk509+VeFko+IMCP1J2wvxLxu1j1giT1TXD6xEmo
PH6STYMhZ6DnpARK3b6XDjRWfq981ExufQKCAQAwPtgINZF5c5GjIyn0EZh5cK4l
Ef7E7qXHFYV9yH4dswE9hdOD6IggaZTv1XvrwH5SN9Kt8FOXbPMsARSlD1kUNWsl
aTuco3xmCQNtq/ydN3OGMKOgUV3egzc1xWKSB8txOnfwZyEoHyCT6EQUQgF0ePLm
jcq9ONsyyLWZnRc7qxfJIwb7zCNAvOQezd+J+sDcqUQShfc3tzhqLmfaEOQz/Bz/
4Sy6OIsujW1LWiJ//B0QXxxhjWd8NKmuTQC1cyKKXUh8iXvAO0CNjhdcZxjN+07n
JSuuwpLFnQtfda1VpNg0seYqbihuuVJpOA55/tlu1BiakdIW6DHB0wrMOL50
-----END RSA PRIVATE KEY-----
"""

server_crt = """
-----BEGIN CERTIFICATE-----
MIIGOjCCBCKgAwIBAgIUPZomw8k4yyMSlXYFUrsQ7Co8LCowDQYJKoZIhvcNAQEL
BQAwgawxCzAJBgNVBAYTAlVTMRAwDgYDVQQIDAdBcml6b25hMRAwDgYDVQQHDAdQ
aG9lbml4MSAwHgYDVQQKDBdBbm9ueW1vdXMgQml0IEZhY3RvcmllczEUMBIGA1UE
CwwLRW5naW5lZXJpbmcxGjAYBgNVBAMMEXBnLXRlc3QubG9jYWxob3N0MSUwIwYJ
KoZIhvcNAQkBFhZmYWtlQHBnLXRlc3QubG9jYWxob3N0MCAXDTIzMDIwNDIwMDEx
MFoYDzIwNzcxMjAxMjAwMTEwWjCBrDELMAkGA1UEBhMCVVMxEDAOBgNVBAgMB0Fy
aXpvbmExEDAOBgNVBAcMB1Bob2VuaXgxIDAeBgNVBAoMF0Fub255bW91cyBCaXQg
RmFjdG9yaWVzMRQwEgYDVQQLDAtFbmdpbmVlcmluZzEaMBgGA1UEAwwRcGctdGVz
dC5sb2NhbGhvc3QxJTAjBgkqhkiG9w0BCQEWFmZha2VAcGctdGVzdC5sb2NhbGhv
c3QwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQCnoLq3d7HCDHlBCN6h
ba+1rBKX1IyGw0p/AZWH3uBK0EIc2pkO47qefmqncabOQMgj4Hy4Ak97nOcdKWqP
aDXcSIUQ15oXcb2XW7NpYYS+4XJTayrHeW7+knPohISGWiCSsbP2KeYxwto0vObh
dtdH/2mN/dAqrvE8fQJpyNLN4D2B8CYKMokz8PGStZ7Iq8Em5UJlsM9sIe1LUwm8
BxTQMv3B18+vj7Y/itWPMGBvNTM9RcsNP97ACT3hok9kBYC+02t1teNK8gn8JYF5
0u3IKkc9yFHBo50muRlgi1hGpg/tIoyvB11NutD/vSwIcJL1irtfo070401WuAuF
VCbdRrcWwbIpxT9vH7ERhCwCwImPs+xZ1GEiZT/dt0eo+hvDIpFuyxrmLPWuvB1D
R4VRqS/6F7eepctSqao6zhGuszk69Gx78kKLk3nQ2VFeSKLHa73T28J09AXO+KJV
BuSmrNKZ6M3Q8XcZgWlzT5zscOnvwn1gR35Yy/HXosAFXL0qrl5hBsZA6ZpbMfTV
8f6He/EacDgb0CtcY8v3c/T7O02Ecl9cGDlvA3PLlxxriujLpA3Hzsz/6fbeIQcT
gGf266VgmxfHa6tQ4A31YIlMphQtN+4gKFYhEPLlx5hVx50eK6FE73RbIaybAgsC
gBXSN5sOiwwrpD6Qig4zYNPWfwIDAQABo1AwTjAdBgNVHQ4EFgQUy0r/yzk92MJZ
skVC5HrZ76nJKHkwHwYDVR0jBBgwFoAUy0r/yzk92MJZskVC5HrZ76nJKHkwDAYD
VR0TBAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAgEAFYbhdj3Tz1E17iXe6dGQluCy
Fdo4PTMO7MwHhrpsscWdFpzJq9dtcWwLyGYIy111WNfB4AHVg8e13Ula5B9mi2CK
7kJjRZ+fFPuoBOG+qhXurf/yDhUwavF/forTCDiL58wc6QzGxp4TmkVyZzus2ryj
WmrgkLYMSzLNbWor/kLZzGh5OCUtLFXjL4EJn4NskbeOPvTotcmsOlokNryiH/t6
ploi0TCL8JjdVblT1uPFtytEiheySJt3SZvL7tQhDBZfhNeup45f1bpQCtPGqqPd
9aTwSaatXNWfIltBpWMiyaj+udD7hntee0pD6iPdXh13knKOwhHzLET4OHEAPZGj
V4hZly5acthz6Xu9WLCznEo9/CZ1pyltKFP2Cx3xpkoGt8GQ3QiLdNvpox0xCVYM
8kQ9XGW3lEdZ+zl02flaN/Mah24RzDFAlceapSJLGg47Lrct+QWNuOo0LlAkA6Ir
XD96B4pjcfHmM1Qg0FROWed0UuDnnqFxM+4tyEnnPfhd6lgkQA8oVNJg8sgSm+Tl
NKdWyaylxx8ElI3e1ebzmfuY+J/DvlCbVd+7ZcPLAtsMqWIFWkWf2fXiLBWAll0Q
wqnIFRifRR6wFjSW2Re3gv64ShYWxqhRYztUSKzDFqJCmOyca/Ou4Yvfo2RJtiMk
kD4TZkFt1F7QewUFoMI=
-----END CERTIFICATE-----
"""

class test_ssl_connect(test_connect.test_connect):
	"""
	Run test_connect, but with SSL.
	"""
	params = {'sslmode' : 'require'}
	cluster_path_suffix = '_test_ssl_connect'

	def configure_cluster(self):
		if not has_ssl:
			return

		super().configure_cluster()
		self.cluster.settings['ssl'] = 'on'
		with open(self.cluster.hba_file, 'a') as hba:
			hba.writelines([
				# nossl user
				"\n",
				"hostnossl test nossl 0::0/0 trust\n",
				"hostnossl test nossl 0.0.0.0/0 trust\n",
				# ssl-only user
				"hostssl test sslonly 0.0.0.0/0 trust\n",
				"hostssl test sslonly 0::0/0 trust\n",
			])
		key_file = os.path.join(self.cluster.data_directory, 'server.key')
		crt_file = os.path.join(self.cluster.data_directory, 'server.crt')
		with open(key_file, 'w') as key:
			key.write(server_key)
		with open(crt_file, 'w') as crt:
			crt.write(server_crt)
		os.chmod(key_file, 0o700)
		os.chmod(crt_file, 0o700)

		self.params['sslrootcrtfile'] = crt_file

	def initialize_database(self):
		if not has_ssl:
			return

		super().initialize_database()
		# Setup TLS users.
		with self.cluster.connection(user = 'test', **self.params) as db:
			db.execute(
				"""
CREATE USER nossl;
CREATE USER sslonly;
				"""
			)

	@unittest.skipIf(default_installation is None, "no installation provided by environment")
	@unittest.skipIf(not has_ssl, "could not detect installation tls")
	def test_ssl_mode_require(self):
		host, port = self.cluster.address()
		params = dict(self.params)
		params['sslmode'] = 'require'
		try:
			pg_driver.connect(
				user = 'nossl',
				database = 'test',
				host = host,
				port = port,
				**params
			)
			self.fail("successful connection to nossl user when sslmode = 'require'")
		except pg_exc.ClientCannotConnectError as err:
			for pq in err.database.failures:
				x = pq.error
				dossl = pq.ssl_negotiation
				if isinstance(x, pg_exc.AuthenticationSpecificationError) and dossl is True:
					break
			else:
				# let it show as a failure.
				raise
		with pg_driver.connect(
			host = host,
			port = port,
			user = 'sslonly',
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, 'ssl')

	@unittest.skipIf(default_installation is None, "no installation provided by environment")
	@unittest.skipIf(not has_ssl, "could not detect installation tls")
	def test_ssl_mode_disable(self):
		host, port = self.cluster.address()
		params = dict(self.params)
		params['sslmode'] = 'disable'
		try:
			pg_driver.connect(
				user = 'sslonly',
				database = 'test',
				host = host,
				port = port,
				**params
			)
			self.fail("successful connection to sslonly user with sslmode = 'disable'")
		except pg_exc.ClientCannotConnectError as err:
			for pq in err.database.failures:
				x = pq.error
				if isinstance(x, pg_exc.AuthenticationSpecificationError) and not hasattr(pq, 'ssl_negotiation'):
					# looking for an authspec error...
					break
			else:
				# let it show as a failure.
				raise

		with pg_driver.connect(
			host = host,
			port = port,
			user = 'nossl',
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, None)

	@unittest.skipIf(default_installation is None, "no installation provided by environment")
	@unittest.skipIf(not has_ssl, "could not detect installation tls")
	def test_ssl_mode_prefer(self):
		host, port = self.cluster.address()
		params = dict(self.params)
		params['sslmode'] = 'prefer'
		with pg_driver.connect(
			user = 'sslonly',
			host = host,
			port = port,
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, 'ssl')

		with pg_driver.connect(
			user = 'test',
			host = host,
			port = port,
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.security, 'ssl')

		with pg_driver.connect(
			user = 'nossl',
			host = host,
			port = port,
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, None)

	@unittest.skipIf(default_installation is None, "no installation provided by environment")
	@unittest.skipIf(not has_ssl, "could not detect installation tls")
	def test_ssl_mode_allow(self):
		host, port = self.cluster.address()
		params = dict(self.params)
		params['sslmode'] = 'allow'

		# nossl user (hostnossl)
		with pg_driver.connect(
			user = 'nossl',
			database = 'test',
			host = host,
			port = port,
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, None)

		# test user (host)
		with pg_driver.connect(
			user = 'test',
			host = host,
			port = port,
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.security, None)

		# sslonly user (hostssl)
		with pg_driver.connect(
			user = 'sslonly',
			host = host,
			port = port,
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, 'ssl')

if __name__ == '__main__':
	unittest.main()
