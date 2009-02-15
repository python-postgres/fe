#!/usr/bin/env python
##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
# Statement I/O: Mass insert and select performance
##
import os
import time
import sys

def insertSamples(count, insert_records):
	recs = [
		(-3, 123, 0xfffffea023, 'some_óäæ_thing', 'varying', 'æ')
		for x in range(count)
	]
	gen = time.time()
	insert_records.load(recs)
	fin = time.time()
	xacttime = fin - gen
	ats = count / xacttime
	sys.stderr.write(
		"INSERT Summary,\n " \
		"inserted tuples: %d\n " \
		"total time: %f\n " \
		"average tuples per second: %f\n " %(
			count, xacttime, ats, 
		)
	)

def timeTupleRead(portal):
	loops = 0
	genesis = time.time()
	for x in portal:
		loops += 1
	finalis = time.time()
	looptime = finalis - genesis
	ats = loops / looptime
	sys.stderr.write(
		"SELECT Summary,\n " \
		"looped/tuples: %d\n " \
		"looptime: %f\n " \
		"average tuples per second: %f\n " %(loops, looptime, ats,)
	)

def main(count):
	execute('CREATE TEMP TABLE samples '
		'(i2 int2, i4 int4, i8 int8, t text, v varchar, c char)')
	insert_records = prepare(
		"INSERT INTO samples VALUES ($1, $2, $3, $4, $5, $6)"
	)
	select_records = prepare("SELECT * FROM samples")
	try:
		insertSamples(count, insert_records)
		timeTupleRead(select_records())	
	finally:
		execute("DROP TABLE samples")

def command(args):
	main(int((args + [25000])[1]))

if __name__ == '__main__':
	command(sys.argv)
