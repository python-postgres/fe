##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
r'''
Gotchas
=======

It is recognized that decisions were made that may not always be ideal for a
given user. In order to highlight those potential issues and hopefully bring
some sense into a confusing situation, this document was drawn.


client_encoding setting should be altered carefully
---------------------------------------------------

`postgresql.driver`'s streaming cursor implementation reads a fixed set of rows
when it queries the server for more. In order to optimize some situations, the
driver will send a request for more data, but makes no attempt to wait and
process the data as it is not yet needed. When the user comes back to read more
data from the cursor, it will then look at this new data. The problem being, if
`client_encoding` was switched, it may use the wrong codec to transform the
wire data into higher level Python objects(str).

To avoid this problem from ever happening, set the `client_encoding` early.
Furthermore, it is probably best to never change the `client_encoding` as the
driver automatically makes the necessary transformation to Python strings.


The user and password is correct, but does it not work when using `postgresql.driver`
-------------------------------------------------------------------------------------

This issue likely comes from the possibility that the information sent to the
server early in the negotiation phase may not be in an encoding that is
consistent with the server's encoding.

One problem is that PostgreSQL does not provide the client with the server
encoding early enough in the negotiation phase, and, therefore, is unable to
process the password data in a way that is consistent with the server's
expectations.

Another problem is that PostgreSQL takes much of the data in the startup message
as-is, so a decision about the best way to encode parameters is difficult.

The easy way to avoid *most* issues with this problem is to initialize the
database in the `utf-8` encoding. The driver defaults the expected server
encoding to `utf-8`. However, this can be overridden by creating the `Connector`
with a `server_encoding` parameter. Setting `server_encoding` to the proper
value of the target server will allow the driver to properly encode *some* of
the parameters. Also, any GUC parameters passed via the `settings` parameter
should use typed objects when possible to hint that the server encoding should
not be used on that parameter(`bytes`, for instance).


Backslash characters are being treated literally
------------------------------------------------

The driver enables standard compliant strings. Stop using non-standard features.
;)

If support for non-standard strings was provided it would require to the
driver to provide subjective quote interfaces(eg, db.quote_literal). Doing so is
not desirable as it introduces difficulties for the driver *and* the user.


Types without binary support in the driver are unsupported in arrays and records
--------------------------------------------------------------------------------

When an array or composite type is identified, `postgresql.protocol.typio`
ultimately chooses the binary format for the transfer of the column or
parameter. When this is done, PostgreSQL will pack or expect *all* the values
in binary format as well. If that binary format is not supported and the type
is not an string, it will fail to unpack the row or pack the appropriate data for
the element or attribute.

In most cases issues related to this can be avoided with explicit casts to text.
'''

__docformat__ = 'reStructuredText'
if __name__ == '__main__':
	import sys
	if (sys.argv + [None])[1] == 'dump':
		sys.stdout.write(__doc__)
	else:
		try:
			help(__package__ + '.gotchas')
		except NameError:
			help(__name__)
