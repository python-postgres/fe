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
	PyObject *typ;
	PyObject *ob;
	PyObject *rob;
	const char *data;
	uint16_t cnatt = 0, natts = 0;
	uint32_t attsize = 0;
	int position = 0;
	int dlen = 0;

	if (PyArg_ParseTuple(args, "Oy#", &typ, &data, &dlen) < 0)
		return(NULL);

	if (typ != Py_None)
	{
		if (!PyObject_IsSubclass(typ, (PyObject *) &PyTuple_Type))
		{
			const char *typname = "<not-a-type>";
			if (PyObject_IsInstance((PyObject *) typ->ob_type, (PyObject *) &PyType_Type))
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
				"not enough data for attribute %d", cnatt);
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
			if (position + attsize > dlen)
			{
				PyErr_Format(PyExc_ValueError,
					"not enough data for attribute %d, size %d, "
					"but only %d remaining bytes in message",
					cnatt, attsize, dlen - position
				);
				goto cleanup;
			}
			ob = PyBytes_FromStringAndSize(data + position, attsize);
			if (ob == NULL)
			{
				/*
				 * Probably a OOM error.
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
			"invalid tuple message, %d remaining "
			"bytes after processing %d attriutes",
			dlen - position, cnatt
		);
		goto cleanup;
	}

	return(rob);

cleanup:
	Py_DECREF(rob);
	return(NULL);
}

static PyMethodDef optimized_methods[] = {
	{"parse_tuple_message", (PyCFunction) parse_tuple_message, METH_VARARGS,
		PyDoc_STR("parse the given tuple data into a tuple of raw data"),},
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
