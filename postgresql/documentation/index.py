##
# copyright 2009, James William Pye
# http://python.projects.postgresql.org
##
r"""
py-postgresql
=============

py-postgresql is a project dedicated to improving the Python interfaces to
PostgreSQL. At its core, py-postgresql provides a PG-API and DB-API 2.0
modules for accessing a PostgreSQL database. The PG-API interface is
recommended as it provides greater utility::

	import postgresql.driver as pg_driver
	db = pg_driver.connect(...)
	db.execute("CREATE TABLE emp (emp_name text PRIMARY KEY, emp_salary numeric)")

	# Create the queries.
	make_emp = db.query("INSERT INTO emp VALUES ($1, $2)")
	remove_emp = db.query("DELETE FROM emp WHERE emp_name = $1")
	get_emp_salary = db.query("SELECT emp_salary FROM emp WHERE emp_name = $1")
	get_emp_with_salary_gt = db.query("SELECT emp_name FROM emp WHERE emp_salay > $1")

	# Create some employees, but do it in a transaction--all or nothing.
	with db.xact:
		make_emp("John Doe", "150,000")
		make_emp("Jane Doe", "150,000")
		make_emp("Andrew Doe", "55,000")
		make_emp("Susan Doe", "60,000")

	# Now print the overpaid employees
	with db.xact:
		for row in get_emp_with_salaray_gt("125,000"):
			print(row["emp_name"])
			# And fire them. ;)
			remove_emp(row["emp_name"])


Of course, if DB-API 2.0 is desired, the module is located at
`postgresql.driver.dbapi20`
"""
__docformat__ = 'reStructuredText'
if __name__ == '__main__':
	import sys
	if (sys.argv + [None])[1] == 'dump':
		sys.stdout.write(__doc__)
	else:
		try:
			help(__package__ + '.index')
		except NameError:
			help(__name__)
