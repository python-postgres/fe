##
# .test.test_alock - test .alock
##
import unittest
import threading
import time
from ..temporal import pg_tmp
from .. import alock

class test_alock(unittest.TestCase):
	@pg_tmp
	def testALockNoWait(self):
		alt = new()
		ad = db.prepare(
			"select count(*) FROM pg_locks WHERE locktype = 'advisory'"
		).first
		self.failUnlessEqual(ad(), 0)
		with alock.ExclusiveLock(db, (0,0)):
			l=alock.ExclusiveLock(alt, (0,0))
			# should fail to acquire
			self.failUnlessEqual(l.acquire(False), False)
		# no alocks should exist now
		self.failUnlessEqual(ad(), 0)

	@pg_tmp
	def testALock(self):
		ad = db.prepare(
			"select count(*) FROM pg_locks WHERE locktype = 'advisory'"
		).first
		self.failUnlessEqual(ad(), 0)
		# test a variety..
		lockids = [
			(1,4),
			-32532, 0, 2,
			(7, -1232),
			4, 5, 232142423,
			(18,7),
			2, (1,4)
		]
		alt = new()
		xal1 = alock.ExclusiveLock(db, *lockids)
		xal2 = alock.ExclusiveLock(db, *lockids)
		sal1 = alock.ShareLock(db, *lockids)
		with sal1:
			with xal1, xal2:
				self.failUnless(ad() > 0)
				for x in lockids:
					xl = alock.ExclusiveLock(alt, x)
					self.failUnlessEqual(xl.acquire(False), False)
				# main has exclusives on these, so this should fail.
				xl = alock.ShareLock(alt, *lockids)
				self.failUnlessEqual(xl.acquire(False), False)
			for x in lockids:
				# sal1 still holds
				xl = alock.ExclusiveLock(alt, x)
				self.failUnlessEqual(xl.acquire(False), False)
				# sal1 still holds, but we want a share lock too.
				xl = alock.ShareLock(alt, x)
				self.failUnlessEqual(xl.acquire(False), True)
				xl.release()
		# no alocks should exist now
		self.failUnlessEqual(ad(), 0)

	@pg_tmp
	def testPartialALock(self):
		# Validates that release is properly cleaning up
		ad = db.prepare(
			"select count(*) FROM pg_locks WHERE locktype = 'advisory'"
		).first
		self.failUnlessEqual(ad(), 0)
		held = (0,-1234)
		wanted = [0, 324, -1232948, 7, held, 1, (2,4), (834,1)]
		alt = new()
		with alock.ExclusiveLock(db, held):
			l=alock.ExclusiveLock(alt, *wanted)
			# should fail to acquire, db has held
			self.failUnlessEqual(l.acquire(False), False)
		# No alocks should exist now.
		# This *MUST* occur prior to alt being closed.
		# Otherwise, we won't be testing for the recovery
		# of a failed non-blocking acquire().
		self.failUnlessEqual(ad(), 0)

	@pg_tmp
	def testALockParameterErrors(self):
		self.failUnlessRaises(TypeError, alock.ALock)
		l = alock.ExclusiveLock(db)
		self.failUnlessRaises(RuntimeError, l.release)

	@pg_tmp
	def testALockOnClosed(self):
		ad = db.prepare(
			"select count(*) FROM pg_locks WHERE locktype = 'advisory'"
		).first
		self.failUnlessEqual(ad(), 0)
		held = (0,-1234)
		alt = new()
		# __exit__ should only touch the count.
		with alock.ExclusiveLock(alt, held) as l:
			self.failUnlessEqual(ad(), 1)
			self.failUnlessEqual(l.locked(), True)
			alt.close()
			time.sleep(0.005)
			self.failUnlessEqual(ad(), 0)
			self.failUnlessEqual(l.locked(), False)

if __name__ == '__main__':
	unittest.main()
