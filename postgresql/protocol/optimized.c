/*
 * copyright 2009, James William Pye
 * http://python.projects.postgresql.org
 *
 *//*
 * Optimizations for protocol modules.
 */
#include <stdint.h>
#ifdef WIN32
#include <winsock.h>
#else
#include <sys/types.h>
#include <netinet/in.h>
#endif
#include <Python.h>
#include <structmember.h>

static PyObject *
parse_tuple_message(PyObject *self, PyObject *args)
{
	PyObject *rob;
	PyObject *ob;
	PyObject *typ;
	const char *data;
	Py_ssize_t dlen = 0;
	uint16_t cnatt = 0, natts = 0;
	uint32_t attsize = 0;
	uint32_t position = 0;

	if (!PyArg_ParseTuple(args, "Oy#", &typ, &data, &dlen))
		return(NULL);

	/*
	 * Validate that the given "typ" is in fact a PyTuple_Type subtype.
	 */
	if (typ != Py_None)
	{
		if (!PyObject_IsSubclass(typ, (PyObject *) &PyTuple_Type))
		{
			const char *typname = "<not-a-type>";
			if (PyObject_IsInstance(
				(PyObject *) typ->ob_type,
				(PyObject *) &PyType_Type
			))
			{
				typname = ((PyTypeObject *) typ)->tp_name;
			}

			PyErr_Format(
				PyExc_TypeError,
				"cannot instantiate wire tuple into a non-tuple subtype: %s",
				typname
			);
			return(NULL);
		}
	}
	else
	{
		typ = (PyObject *) &PyTuple_Type;
	}

	if (dlen < 2)
	{
		PyErr_Format(PyExc_ValueError,
			"invalid tuple message: %d bytes is too small", dlen);
		return(NULL);
	}
	natts = ntohs(*((uint16_t *) (data)));

	/*
	 * FEARME: A bit much for saving a reallocation/copy?
	 *
	 * This is expected to be used as a classmethod on a tuple subtype that
	 * has *no* additional attributes.
	 *
	 * If the subtype has a custom __new__ routine, this could
	 * be problematic, but it *should* only lead to AttributeErrors.
	 */
	rob = ((PyTypeObject *) typ)->tp_alloc((PyTypeObject *) typ, natts); 
	if (rob == NULL)
	{
		return(NULL);
	}

	position += 2;
	while (cnatt < natts)
	{
		/*
		 * Need enough data for the attribute size.
		 */
		if (position + 4 > dlen)
		{
			PyErr_Format(PyExc_ValueError,
				"not enough data available for attribute %d's size header: "
				"needed %d bytes, but only %lu remain at position %lu",
				cnatt, 4, dlen - position, position
			);
			goto cleanup;
		}

		attsize = ntohl(*((uint32_t *) (data + position)));
		position += 4;
		/*
		 * NULL.
		 */
		if (attsize == 0xFFFFFFFFL)
		{
			Py_INCREF(Py_None);
			PyTuple_SET_ITEM(rob, cnatt, Py_None);
		}
		else
		{
			if ((position + attsize) < position)
			{
				/*
				 * Likely a "limitation" over the pure-Python version, *but*
				 * the message content size is limited to 0xFFFFFFFF-4 anyways,
				 * so it is unexpected for an attsize to cause wrap-around.
				 */
				PyErr_Format(PyExc_ValueError,
					"tuple data caused position (uint32_t) "
					"to wrap on attribute %d, position %lu + size %lu",
					cnatt, position, attsize
				);
				goto cleanup;
			}

			if (position + attsize > dlen)
			{
				PyErr_Format(PyExc_ValueError,
					"not enough data for attribute %d, size %lu, "
					"as only %lu bytes remain in message",
					cnatt, attsize, dlen - position
				);
				goto cleanup;
			}

			ob = PyBytes_FromStringAndSize(data + position, attsize);
			if (ob == NULL)
			{
				/*
				 * Probably an OOM error.
				 */
				goto cleanup;
			}
			PyTuple_SET_ITEM(rob, cnatt, ob);
			position += attsize;
		}

		cnatt++;
	}

	if (position != dlen)
	{
		PyErr_Format(PyExc_ValueError,
			"invalid tuple message, %lu remaining "
			"bytes after processing %d attributes",
			dlen - position, cnatt
		);
		goto cleanup;
	}

	return(rob);

cleanup:
	Py_DECREF(rob);
	return(NULL);
}

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

static PyMethodDef optimized_methods[] = {
	{"parse_tuple_message", (PyCFunction) parse_tuple_message, METH_VARARGS,
		PyDoc_STR("parse the given tuple data into a tuple of raw data"),},
	{"process_tuple", (PyCFunction) process_tuple, METH_VARARGS,
		PyDoc_STR(
			"process the items in the second argument "
			"with the corresponding items in the first argument."
		),
	},
	{NULL}
};

static struct PyModuleDef optimized_module = {
   PyModuleDef_HEAD_INIT,
   "optimized",/* name of module */
   NULL,     /* module documentation, may be NULL */
   -1,       /* size of per-interpreter state of the module,
                or -1 if the module keeps state in global variables. */
   optimized_methods,
};

PyMODINIT_FUNC
PyInit_optimized(void)
{
	return(PyModule_Create(&optimized_module));
}
/*
 * vim: ts=3:sw=3:noet:
 */
