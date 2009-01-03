# -*- encoding: utf-8 -*-
##
# copyright 2005, pg/python project.
# http://python.projects.postgresql.org
##
'PostgreSQL bytea encoding and decoding functions'
import codecs
import struct

byte = struct.Struct('B').pack

def bchr(ord):
	if not (32 < ord < 126):
		return b"\\" + oct(ord).lstrip('0').rjust(3, '0')
	elif ord == 92:
		return br'\\'
	else:
		return byte(ord)

def encode(data):
	return ''.join([bchr(ord(x)) for x in data])

def decode(data):
	diter = iter(data)
	output = []
	next = diter.next
	for x in diter:
		if x == "\\":
			try:
				y = next()
			except StopIteration:
				raise ValueError("incomplete backslash sequence")
			if y == "\\":
				# It's a backslash, so let x(\) be appended.
				pass
			elif y.isdigit():
				try:
					os = ''.join((y, next(), next()))
				except StopIteration:
					# requires three digits
					raise ValueError("incomplete backslash sequence")
				try:
					x = chr(int(os, base = 8))
				except ValueError:
					raise ValueError("invalid bytea octal sequence '%s'" %(os,))
			else:
				raise ValueError("invalid backslash follow '%s'" %(y,))
		output.append(x)
	return ''.join(output)

class Codec(codecs.Codec):
	'bytea codec'
	def encode(data, errors = 'strict'):
		return (encode(data), len(data))
	encode = staticmethod(encode)

	def decode(data, errors = 'strict'):
		return (decode(data), len(data))
	decode = staticmethod(decode)

class StreamWriter(Codec, codecs.StreamWriter): pass
class StreamReader(Codec, codecs.StreamReader): pass
