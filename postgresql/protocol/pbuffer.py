##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
__all__ = ['pq_message_stream']

from io import BytesIO
import struct
from .message_types import message_types

xl_unpack = struct.Struct('!xL').unpack_from

class pq_message_stream(object):
	'provide a message stream from a data stream'
	_block = 512
	_limit = _block * 4
	def __init__(self):
		self._strio = BytesIO()
		self._start = 0

	def truncate(self):
		"remove all data in the buffer"
		self._strio.truncate(0)
		self._start = 0

	def _rtruncate(self, amt = None):
		"[internal] remove the given amount of data"
		if amt is None:
			amt = self._strio.tell()
		self._strio.seek(0, 2)
		size = self._strio.tell()
		# if the total size is equal to the amt,
		# then the whole thing is going to be truncated.
		if size == amt:
			self._strio.truncate(0)
			return

		copyto_pos = 0
		copyfrom_pos = amt
		while True:
			self._strio.seek(copyfrom_pos)
			data = self._strio.read(self._block)
			# Next copyfrom
			copyfrom_pos = self._strio.tell()
			self._strio.seek(copyto_pos)
			self._strio.write(data)
			if len(data) != self._block:
				break
			# Next copyto
			copyto_pos = self._strio.tell()

		self._strio.truncate(size - amt)

	def has_message(self):
		"if the buffer has a message available"
		self._strio.seek(self._start)
		header = self._strio.read(5)
		if len(header) < 5:
			return False
		length, = xl_unpack(header)
		if length < 4:
			raise ValueError("invalid message size '%d'" %(length,))
		self._strio.seek(0, 2)
		return (self._strio.tell() - self._start) >= length + 1

	def __len__(self):
		"number of messages in buffer"
		count = 0
		rpos = self._start
		self._strio.seek(self._start)
		while True:
			# get the message metadata
			header = self._strio.read(5)
			rpos += 5
			if len(header) < 5:
				# not enough data for another message
				break
			# unpack the length from the header
			length, = xl_unpack(header)
			rpos += length - 4

			if length < 4:
				raise ValueError("invalid message size '%d'" %(length,))
			self._strio.seek(length - 4 - 1, 1)

			if len(self._strio.read(1)) != 1:
				break
			count += 1
		return count

	def _get_message(self, mtypes = message_types):
		header = self._strio.read(5)
		if len(header) < 5:
			return
		length, = xl_unpack(header)
		typ = mtypes[header[0]]

		if length < 4:
			raise ValueError("invalid message size '%d'" %(length,))
		length -= 4
		body = self._strio.read(length)
		if len(body) < length:
			# Not enough data for message.
			return
		return (typ, body)

	def next_message(self):
		if self._start > self._limit:
			self._rtruncate(self._start)
			self._start = 0

		self._strio.seek(self._start)
		msg = self._get_message()
		if msg is not None:
			self._start = self._strio.tell()
		return msg

	def __next__(self):
		if self._start > self._limit:
			self._rtruncate(self._start)
			self._start = 0

		self._strio.seek(self._start)
		msg = self._get_message()
		if msg is None:
			raise StopIteration
		self._start = self._strio.tell()
		return msg

	def read(self, num = 0xFFFFFFFF):
		if self._start > self._limit:
			self._rtruncate(self._start)
			self._start = 0

		self._strio.seek(self._start)
		l = []
		new_start = self._start
		while len(l) < num:
			msg = self._get_message()
			if msg is None:
				break
			l.append(msg)
			new_start += (5 + len(msg[1]))
		self._start = new_start
		return l

	def write(self, data):
		# Always append data; it's a stream, damnit..
		self._strio.seek(0, 2)
		self._strio.write(data)
