##
# copyright 2008, pg/python project.
# http://python.projects.postgresql.org
##
# Copy I/O: To and From performance
##
import os, sys, gc, random, time

if __name__ == '__main__':
	Words = open('/usr/share/dict/words').readlines()
else:
	Words = ['/usr/share/dict/words', 'is', 'read', 'in', '__main__']
wordcount = len(Words)
random.seed()

def getWord():
	"extract a random word from ``Words``"
	return Words[random.randrange(0, wordcount)].strip()

def testSpeed(tuples = 50000 * 3):
	execute("CREATE TEMP TABLE _copy "
	"(i int, t text, mt text, ts text, ty text, tx text);")
	try:
		Q = query("COPY _copy FROM STDIN")
		size = [0]
		def incsize(data):
			size[0] += len(data)
			return data
		sys.stderr.write("preparing data(%d tuples)...\n" %(tuples,))

		# Use an LC to avoid the Python overhead involved with a GE
		data = [incsize('\t'.join((
			str(x), getWord(), getWord(),
			getWord(), getWord(), getWord()
		)))+'\n' for x in range(tuples)]

		sys.stderr.write("starting copy...\n")
		start = time.time()
		copied_in = Q(data)
		duration = time.time() - start
		sys.stderr.write(
			"COPY FROM STDIN Summary,\n " \
			"copied tuples: %d\n " \
			"copied bytes: %d\n " \
			"duration: %f\n " \
			"average tuple size(bytes): %f\n " \
			"average KB per second: %f\n " \
			"average tuples per second: %f\n" %(
				tuples, size[0], duration,
				size[0] / tuples,
				size[0] / 1024 / duration,
				tuples / duration, 
			)
		)
		Q = query("COPY _copy TO STDOUT")
		start = time.time()
		c = 0
		for x in Q():
			c += 1
		duration = time.time() - start
		sys.stderr.write(
			"COPY TO STDOUT Summary,\n " \
			"copied tuples: %d\n " \
			"duration: %f\n " \
			"average KB per second: %f\n " \
			"average tuples per second: %f\n " %(
				c, duration,
				size[0] / 1024 / duration,
				tuples / duration, 
			)
		)
	finally:
		execute("DROP TABLE _copy")

if __name__ == '__main__':
	testSpeed()
