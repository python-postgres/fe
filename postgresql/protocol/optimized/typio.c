/*
 * copyright 2009, James William Pye
 * http://python.projects.postgresql.org
 *
 */
#define include_typio_functions \
	mFUNC(process_tuple, METH_VARARGS, \
		"process the items in the second argument " \
		"with the corresponding items in the first argument.") \

/*
 * process the tuple with the associated callables while
 * calling the third object in cases of failure to generalize the exception.
 */
static PyObject *
process_tuple(PyObject *self, PyObject *args)
{
	PyObject *tup, *procs, *fail, *rob;
	Py_ssize_t len, i;

	if (!PyArg_ParseTuple(args, "OOO", &procs, &tup, &fail))
		return(NULL);

	if (!PyObject_IsInstance(procs, (PyObject *) &PyTuple_Type))
	{
		PyErr_SetString(
			PyExc_TypeError,
			"process_tuple requires a tuple as its first argument"
		);
		return(NULL);
	}

	if (!PyObject_IsInstance(tup, (PyObject *) &PyTuple_Type))
	{
		PyErr_SetString(
			PyExc_TypeError,
			"process_tuple requires a tuple as its second argument"
		);
		return(NULL);
	}

	len = PyTuple_GET_SIZE(tup);

	if (len != PyTuple_GET_SIZE(procs))
	{
		PyErr_Format(
			PyExc_ValueError,
			"inconsistent items, %d processors and %d objects",
			len,
			PyTuple_GET_SIZE(procs)
		);
		return(NULL);
	}
	rob = PyTuple_New(len);

	for (i = 0; i < len; ++i)
	{
		PyObject *p, *o, *ot, *r;
		/*
		 * If it's Py_None, that means it's NULL. No processing necessary.
		 */
		o = PyTuple_GET_ITEM(tup, i);
		if (o == Py_None)
		{
			Py_INCREF(Py_None);
			PyTuple_SET_ITEM(rob, i, Py_None);
			continue;
		}

		p = PyTuple_GET_ITEM(procs, i);
		/*
		 * Temp tuple for applying *args to p.
		 */
		ot = PyTuple_New(1);
		PyTuple_SET_ITEM(ot, 0, o);
		Py_INCREF(o);

		r = PyObject_CallObject(p, ot);
		Py_DECREF(ot);
		if (r == NULL)
		{
			/*
			 * Exception from p(*ot)
			 */
			Py_DECREF(rob);
			rob = NULL;

			/*
			 * Don't trap BaseException's.
			 */
			if (PyErr_ExceptionMatches(PyExc_Exception))
			{
				PyObject *failargs, *failedat;
				PyObject *exc, *val, *tb;
				PyObject *oldexc, *oldval, *oldtb;

				/* Store exception to set context after handler. */
				PyErr_Fetch(&oldexc, &oldval, &oldtb);
				PyErr_NormalizeException(&oldexc, &oldval, &oldtb);

				failedat = PyLong_FromSsize_t(i);
				if (failedat != NULL)
				{
					failargs = PyTuple_New(3);
					if (failargs != NULL)
					{
						/* args for the exception "handler" */
						PyTuple_SET_ITEM(failargs, 0, procs);
						Py_INCREF(procs);
						PyTuple_SET_ITEM(failargs, 1, tup);
						Py_INCREF(tup);
						PyTuple_SET_ITEM(failargs, 2, failedat);

						r = PyObject_CallObject(fail, failargs);
						Py_DECREF(failargs);
						if (r != NULL)
						{
							PyErr_SetString(PyExc_RuntimeError,
								"process_tuple exception handler failed to raise"
							);
							Py_DECREF(r);
						}
					}
					else
					{
						Py_DECREF(failedat);
					}
				}

				PyErr_Fetch(&exc, &val, &tb);
				PyErr_NormalizeException(&exc, &val, &tb);

				/*
				 * Reference BaseException here as the condition is merely
				 * *validating* that SetContext can be used.
				 */
				if (val != NULL && PyObject_IsInstance(val, PyExc_BaseException))
				{
					/* Steals oldval reference */
					PyException_SetContext(val, oldval);
					Py_XDECREF(oldexc);
					Py_XDECREF(oldtb);
					PyErr_Restore(exc, val, tb);
				}
				else
				{
					/*
					 * Fetch & NormalizeException failed somehow.
					 * Use the old exception...
					 */
					PyErr_Restore(oldexc, oldval, oldtb);
					Py_XDECREF(exc);
					Py_XDECREF(val);
					Py_XDECREF(tb);
				}
			}

			/*
			 * Break out of loop to return(NULL);
			 */
			break;
		}
		PyTuple_SET_ITEM(rob, i, r);
	}

	return(rob);
}

